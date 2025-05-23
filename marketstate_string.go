// Code generated by "stringer -type=MarketState"; DO NOT EDIT.

package igmarkets

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[UNKNOWN_MARKET_STATE-0]
	_ = x[CLOSED-1]
	_ = x[OFFLINE-2]
	_ = x[TRADEABLE-3]
	_ = x[EDIT-4]
	_ = x[AUCTION-5]
	_ = x[AUCTION_NO_EDIT-6]
	_ = x[AUCTION_NO_SUSPENDED-7]
}

const _MarketState_name = "UNKNOWN_MARKET_STATECLOSEDOFFLINETRADEABLEEDITAUCTIONAUCTION_NO_EDITAUCTION_NO_SUSPENDED"

var _MarketState_index = [...]uint8{0, 20, 26, 33, 42, 46, 53, 68, 88}

func (i MarketState) String() string {
	if i < 0 || i >= MarketState(len(_MarketState_index)-1) {
		return "MarketState(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _MarketState_name[_MarketState_index[i]:_MarketState_index[i+1]]
}
