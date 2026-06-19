package domain

import "time"

type BillingPlan string

const (
	PlanHobby BillingPlan = "hobby"
	PlanPro   BillingPlan = "pro"
	PlanUltra BillingPlan = "ultra"
)

type BillingStatus string

const (
	BillingStatusActive   BillingStatus = "active"
	BillingStatusPastDue  BillingStatus = "past_due"
	BillingStatusCanceled BillingStatus = "canceled"
	BillingStatusTrialing BillingStatus = "trialing"
)

type BillingSubscription struct {
	ID               string        `json:"id"`
	UserID           string        `json:"user_id,omitempty"` // For personal projects
	TeamID           string        `json:"team_id,omitempty"` // For team projects
	Plan             BillingPlan   `json:"plan"`
	Status           BillingStatus `json:"status"`
	StripeCustomerID string        `json:"stripe_customer_id"`
	CreatedAt        time.Time     `json:"created_at"`
	UpdatedAt        time.Time     `json:"updated_at"`
}

func NewBillingSubscription(id, userID, teamID string, plan BillingPlan, stripeID string) *BillingSubscription {
	return &BillingSubscription{
		ID:               id,
		UserID:           userID,
		TeamID:           teamID,
		Plan:             plan,
		Status:           BillingStatusTrialing,
		StripeCustomerID: stripeID,
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}
}

type PaymentMethod struct {
	ID        string    `json:"id"`
	ContextID string    `json:"context_id"` // User or Team ID
	Brand     string    `json:"brand"`
	Last4     string    `json:"last4"`
	Exp       string    `json:"exp"`
	IsDefault bool      `json:"is_default"`
	CreatedAt time.Time `json:"created_at"`
}

type Invoice struct {
	ID        string    `json:"id"`
	ContextID string    `json:"context_id"`
	Amount    string    `json:"amount"`
	Status    string    `json:"status"`
	PdfURL    string    `json:"pdf_url"`
	CreatedAt time.Time `json:"created_at"`
}
