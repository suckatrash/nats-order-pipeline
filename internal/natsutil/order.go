package natsutil

import "time"

// Order represents a single order in the pipeline.
type Order struct {
	ID        string    `json:"id"`
	Customer  string    `json:"customer"`
	Product   string    `json:"product"`
	Quantity  int       `json:"quantity"`
	Price     float64   `json:"price"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// OrderEnrichment contains customer context data attached to a processed order.
type OrderEnrichment struct {
	OrderID     string    `json:"order_id"`
	Customer    string    `json:"customer"`
	Location    string    `json:"location"`
	Preferences []string  `json:"preferences"`
	Referral    string    `json:"referral"`
	RepeatBuys  int       `json:"repeat_buys"`
	EnrichedAt  time.Time `json:"enriched_at"`
}

// AnalyticsSummary is a periodic summary of order activity.
type AnalyticsSummary struct {
	WindowStart     time.Time `json:"window_start"`
	WindowEnd       time.Time `json:"window_end"`
	TotalOrders     int       `json:"total_orders"`
	ProcessedOrders int       `json:"processed_orders"`
	RejectedOrders  int       `json:"rejected_orders"`
	TotalRevenue    float64   `json:"total_revenue"`
}
