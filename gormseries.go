package gormseries

import (
	"fmt"
	"strings"

	"github.com/jinzhu/gorm"
)

const generateSeriesSQLTemplate = `
	RIGHT JOIN LATERAL ( 
		SELECT * from generate_series(
			%v, %v, %v
		) %v 
	) series ON %v`

type SeriesRange interface {
	StartDate() string
	EndDate() string
	StepInterval() string
	StepName() string
	DefaultJoinCond() string
}

var (
	DaysYear   SeriesRange = new(daysYear)
	MonthsYear SeriesRange = new(monthsYear)
)

type daysYear struct{}

func (d daysYear) StartDate() string {
	return "date_trunc('year', now())"
}

func (d daysYear) EndDate() string {
	return "date_trunc('year', now()) + INTERVAL '1 year - 1 day'"
}

func (d daysYear) StepInterval() string {
	return "INTERVAL '1 day'"
}

func (d daysYear) StepName() string {
	return "day"
}

func (d daysYear) DefaultJoinCond() string {
	return "day = date_trunc('day', created_at)"
}

type monthsYear struct{}

func (d monthsYear) StartDate() string {
	return "date_trunc('year', now())"
}

func (d monthsYear) EndDate() string {
	return "date_trunc('year', now()) + INTERVAL '1 year - 1 day'"
}

func (d monthsYear) StepInterval() string {
	return "INTERVAL '1 month'"
}

func (d monthsYear) StepName() string {
	return "month"
}

func (d monthsYear) DefaultJoinCond() string {
	return "month = date_trunc('month', created_at)"
}

type SeriesDB struct {
	gorm.DB
	series SeriesRange
	args   []interface{}
}

func NewSeriesDB(db gorm.DB) *SeriesDB {
	return &SeriesDB{
		DB: db,
	}
}

// args:
//		1. No args .... default value: date_trunc('{time_unit}', created_at) = day
//		2. Simple On Clause ex 'day = created_at'
func (sdb *SeriesDB) TimeSeries(series SeriesRange, args ...interface{}) *gorm.DB {
	sdb.series = series
	sdb.args = args
	return sdb.BuildScope()
}

// func (sdb *SeriesDB) CustomTimeSeries(r SeriesRange, args ...interface{}) *SeriesDB {

// }

func (sdb *SeriesDB) BuildScope() *gorm.DB {
	var clause string
	if len(sdb.args) > 0 {
		val, ok := sdb.args[0].(string)
		if ok {
			clause = val
			if strings.Contains(clause, "=") {
				clauseParts := strings.Split(clause, "=")
				part1 := dateTruncFmt(sdb.series.StepName(), clauseParts[0])
				part2 := dateTruncFmt(sdb.series.StepName(), clauseParts[1])
				clause = fmt.Sprintf("%v = %v", part1, part2)
			}
		} else {
			clause = sdb.series.DefaultJoinCond() //fmt.Sprintf("%v = date_trunc('%v', created_at)", sdb.series.StepName(), sdb.series.StepName())
		}
	} else {
		clause = sdb.series.DefaultJoinCond() //fmt.Sprintf("%v = date_trunc('%v', created_at)", sdb.series.StepName(), sdb.series.StepName())
	}

	// apply scope
	scope := compileSeriesScopeFromRange(sdb.series, clause)
	return sdb.DB.Scopes(scope)
}

func compileSeriesScopeFromRange(r SeriesRange, onClause string) func(db *gorm.DB) *gorm.DB {
	genSeriesSQL := fmt.Sprintf(generateSeriesSQLTemplate,
		r.StartDate(), r.EndDate(), r.StepInterval(), r.StepName(), onClause)
	orderSQL := fmt.Sprintf("%v ASC", r.StepName())
	return func(db *gorm.DB) *gorm.DB {
		return db.Joins(genSeriesSQL).Order(orderSQL)
	}
}

func dateTruncFmt(precision, date string) string {
	date = strings.TrimSpace(date)
	if !strings.Contains(date, "date_trunc") {
		date = fmt.Sprintf("date_trunc('%v', %v)", precision, date)
	}
	return date
}
