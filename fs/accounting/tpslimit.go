package accounting

import (
	"context"

	"github.com/rclone/rclone/fs"
	"golang.org/x/time/rate"
)

var (
	tpsBucket        *rate.Limiter // for limiting number of http transactions per second
	tpsBucketGeneral *rate.Limiter
	tpsBucketUpload  *rate.Limiter
)

// StartLimitTPS starts the token bucket for transactions per second
// limiting if necessary
func StartLimitTPS(ctx context.Context) {
	ci := fs.GetConfig(ctx)
	if ci.TPSLimit > 0 {
		tpsBurst := ci.TPSLimitBurst
		if tpsBurst < 1 {
			tpsBurst = 1
		}
		tpsBucket = rate.NewLimiter(rate.Limit(ci.TPSLimit), tpsBurst)
		fs.Infof(nil, "Starting transaction limiter: max %g transactions/s with burst %d", ci.TPSLimit, tpsBurst)
	}
}

// LimitTPS limits the number of transactions per second if enabled.
// It should be called once per transaction.
func LimitTPS(ctx context.Context) {
	if tpsBucket != nil {
		tbErr := tpsBucket.Wait(ctx)
		if tbErr != nil && tbErr != context.Canceled {
			fs.Errorf(nil, "HTTP token bucket error: %v", tbErr)
		}
	}
}

func StartLimitTPSGeneral(ctx context.Context) {
	ci := fs.GetConfig(ctx)
	if ci.TPSLimitGeneral > 0 {
		tpsBurstGeneral := ci.TPSLimitBurstGeneral
		if tpsBurstGeneral < 1 {
			tpsBurstGeneral = 1
		}
		tpsBucketGeneral = rate.NewLimiter(rate.Limit(ci.TPSLimitGeneral), tpsBurstGeneral)
		fs.Infof(nil, "Starting transaction limiter for Box General API calls: max %g transactions/s with burst %d", ci.TPSLimitGeneral, tpsBurstGeneral)
	}
}

// LimitTPS for Box General API calls
func LimitTPSGeneral(ctx context.Context) {
	if tpsBucketGeneral != nil {
		tbErr := tpsBucketGeneral.WaitN(ctx, 1)
		if tbErr != nil && tbErr != context.Canceled {
			fs.Errorf(nil, "HTTP token bucket error: %v", tbErr)
		}
	}
}

func StartLimitTPSUpload(ctx context.Context) {
	ci := fs.GetConfig(ctx)
	if ci.TPSLimitUpload > 0 {
		tpsBurstUpload := ci.TPSLimitBurstUpload
		if tpsBurstUpload < 1 {
			tpsBurstUpload = 1
		}
		tpsBucketUpload = rate.NewLimiter(rate.Limit(ci.TPSLimitUpload), tpsBurstUpload)
		fs.Infof(nil, "Starting transaction limiter for Box Upload API Calls: max %g transactions/s with burst %d", ci.TPSLimitUpload, tpsBurstUpload)
	}
}

func LimitTPSUpload(ctx context.Context) {
	if tpsBucketUpload != nil {
		tbErr := tpsBucketUpload.Wait(ctx)
		if tbErr != nil && tbErr != context.Canceled {
			fs.Errorf(nil, "HTTP token bucket error: %v", tbErr)
		}
	}
}
