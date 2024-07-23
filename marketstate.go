package igmarkets

type MarketState int

const (
	UNKNOWN_MARKET_STATE MarketState = iota
	CLOSED
	OFFLINE
	TRADEABLE
	EDIT
	AUCTION
	AUCTION_NO_EDIT
	AUCTION_NO_SUSPENDED
)

func (m MarketState) Parse(s string) (ms MarketState) {
	switch s {
	case "CLOSED":
		ms = CLOSED
	case "OFFLINE":
		ms = OFFLINE
	case "TRADEABLE":
		ms = TRADEABLE
	case "EDIT":
		ms = EDIT
	case "AUCTION":
		ms = AUCTION
	case "AUCTION_NO_EDIT":
		ms = AUCTION_NO_EDIT
	case "AUCTION_NO_SUSPENDED":
		ms = AUCTION_NO_SUSPENDED
	}
	return
}
