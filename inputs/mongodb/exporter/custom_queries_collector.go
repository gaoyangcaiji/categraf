// mongodb_exporter
// Copyright (C) 2017 Percona LLC
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

package exporter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"flashcat.cloud/categraf/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var _ prometheus.Collector = (*customQueriesCollector)(nil)

type QueryConfig struct {
	Mesurement              string          `toml:"mesurement"`
	LabelFields             []string        `toml:"label_fields"`
	EnableExpiredTimeFilter bool            `toml:"enable_expired_time_filter"`
	Filter                  string          `toml:"filter"`
	Database                string          `toml:"database"`
	Timeout                 config.Duration `toml:"timeout"`
	Collections             []string        `toml:"collections"`
}

type customQueriesCollector struct {
	ctx  context.Context
	base *baseCollector

	compatibleMode bool
	topologyInfo   labelsGetter
	queriesInfo    []QueryConfig
}

// newCollectionStatsCollector creates a collector for statistics about collections.
func newCustomQueriesCollector(ctx context.Context, client *mongo.Client, logger *logrus.Logger, compatible bool,
	topology labelsGetter, queries []QueryConfig) *customQueriesCollector {
	return &customQueriesCollector{
		ctx:            ctx,
		base:           newBaseCollector(client, logger),
		compatibleMode: compatible,
		topologyInfo:   topology,
		queriesInfo:    queries,
	}
}

func (d *customQueriesCollector) Describe(ch chan<- *prometheus.Desc) {
	d.base.Describe(d.ctx, ch, d.collect)
}

func (d *customQueriesCollector) Collect(ch chan<- prometheus.Metric) {
	d.base.Collect(ch, d.collect)
}

func (d *customQueriesCollector) collect(ch chan<- prometheus.Metric) {

	queries := d.queriesInfo

	wg := new(sync.WaitGroup)
	defer wg.Wait()

	for _, item := range queries {
		wg.Add(1)
		go d.gatherOneQuery(wg, item, ch)
	}
}

func (d *customQueriesCollector) gatherOneQuery(wg *sync.WaitGroup, query QueryConfig, ch chan<- prometheus.Metric) {
	client := d.base.client
	logger := d.base.logger

	defer wg.Done()

	timeout := time.Duration(query.Timeout)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for _, collName := range query.Collections {
		//parse filter
		filter, err := parseToBsonD(query)
		if err != nil {
			logger.Errorf("failed to parse filter, mesurement:%s for reason: %s", query.Mesurement, err)
			break
		}

		// parse projections
		projection := bson.M{}
		for _, field := range query.LabelFields {
			projection[field] = 1
		}
		opts := options.Find().SetProjection(projection)

		//get metrics data
		collection := client.Database(query.Database).Collection(collName)
		cursor, err := collection.Find(ctx, filter, opts)

		if ctx.Err() == context.DeadlineExceeded {
			logger.Errorf("query timeout, collection:", collName)
			continue
		}

		if err != nil {
			logger.Errorf("cannot get db %s collection %s expire info for reason: %s", query.Database, collName, err)
			continue
		}

		var result []bson.M
		if err = cursor.All(d.ctx, &result); err != nil {
			logger.Errorf("cannot get db %s collection %s expire info for reason: %s", query.Database, collName, err)
			continue
		}
		debugResult(logger, result)

		prefix := query.Mesurement
		labels := d.topologyInfo.baseLabels()
		labels["database"] = query.Database
		labels["collection"] = collName

		for _, metrics := range result {

			for _, label := range query.LabelFields {

				labelValue, has := metrics[label]
				if has {
					labels[label] = fmt.Sprint(labelValue)
					//= strings.Replace(labelValue.(string), " ", "_", -1)
				}
			}

			for _, metric := range makeMetrics(prefix, metrics, labels, d.compatibleMode) {
				ch <- metric
			}
		}
	}

}
