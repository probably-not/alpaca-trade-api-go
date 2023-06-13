package marketdata

import (
	"fmt"
	"net/url"
	"time"
)

type basePaginatedRequest struct {
	baseRequest
	PageToken string
}

func (c *Client) setBasePaginatedQuery(q url.Values, req basePaginatedRequest) {
	c.setBaseQuery(q, req.baseRequest)
	if req.PageToken != "" {
		q.Set("page_token", req.PageToken)
	}
}

// GetTradesPaginatedRequest contains optional parameters for getting trades in a paginated way.
type GetTradesPaginatedRequest struct {
	GetTradesRequest
	// PageToken is the pagination token to continue from
	PageToken string
}

// GetTradesPaginated returns the trades for the given symbol, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetTradesPaginated(symbol string, req GetTradesPaginatedRequest) ([]Trade, string, error) {
	resp, nextPageToken, err := c.GetMultiTradesPaginated([]string{symbol}, req)
	if err != nil {
		return nil, nextPageToken, err
	}
	return resp[symbol], nextPageToken, nil
}

// GetMultiTradesPaginated returns trades for the given symbols, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetMultiTradesPaginated(symbols []string, req GetTradesPaginatedRequest) (map[string][]Trade, string, error) {
	u, err := url.Parse(fmt.Sprintf("%s/%s/trades", c.opts.BaseURL, stockPrefix))
	if err != nil {
		return nil, "", err
	}

	q := u.Query()
	c.setBasePaginatedQuery(q, basePaginatedRequest{
		baseRequest: baseRequest{
			Symbols:  symbols,
			Start:    req.Start,
			End:      req.End,
			Feed:     req.Feed,
			AsOf:     req.AsOf,
			Currency: req.Currency,
		},
		PageToken: req.PageToken,
	})

	trades := make(map[string][]Trade, len(symbols))
	received := 0
	nextPageToken := req.PageToken
	for req.TotalLimit == 0 || received < req.TotalLimit {
		setQueryLimit(q, req.TotalLimit, req.PageLimit, received, v2MaxLimit)
		u.RawQuery = q.Encode()

		resp, err := c.get(u)
		if err != nil {
			return nil, nextPageToken, err
		}

		var tradeResp multiTradeResponse
		if err = unmarshal(resp, &tradeResp); err != nil {
			return nil, nextPageToken, err
		}

		for symbol, t := range tradeResp.Trades {
			trades[symbol] = append(trades[symbol], t...)
			received += len(t)
		}
		if tradeResp.NextPageToken == nil {
			nextPageToken = ""
			break
		}
		nextPageToken = *tradeResp.NextPageToken
		q.Set("page_token", *tradeResp.NextPageToken)
	}
	return trades, "", nil
}

// GetQuotesPaginatedRequest contains optional parameters for getting quotes in a paginated way
type GetQuotesPaginatedRequest struct {
	GetQuotesRequest
	// PageToken is the pagination token to continue from
	PageToken string
}

// GetQuotesPaginated returns quotes for the given symbol, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetQuotesPaginated(symbol string, req GetQuotesPaginatedRequest) ([]Quote, string, error) {
	resp, nextPageToken, err := c.GetMultiQuotesPaginated([]string{symbol}, req)
	if err != nil {
		return nil, nextPageToken, err
	}
	return resp[symbol], nextPageToken, nil
}

// GetMultiQuotesPaginated returns quotes for the given symbols, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetMultiQuotesPaginated(symbols []string, req GetQuotesPaginatedRequest) (map[string][]Quote, string, error) {
	u, err := url.Parse(fmt.Sprintf("%s/%s/quotes", c.opts.BaseURL, stockPrefix))
	if err != nil {
		return nil, "", err
	}

	q := u.Query()
	c.setBasePaginatedQuery(q, basePaginatedRequest{
		baseRequest: baseRequest{
			Symbols:  symbols,
			Start:    req.Start,
			End:      req.End,
			Feed:     req.Feed,
			AsOf:     req.AsOf,
			Currency: req.Currency,
		},
		PageToken: req.PageToken,
	})

	quotes := make(map[string][]Quote, len(symbols))
	received := 0
	nextPageToken := req.PageToken
	for req.TotalLimit == 0 || received < req.TotalLimit {
		setQueryLimit(q, req.TotalLimit, req.PageLimit, received, v2MaxLimit)
		u.RawQuery = q.Encode()

		resp, err := c.get(u)
		if err != nil {
			return nil, nextPageToken, err
		}

		var quoteResp multiQuoteResponse
		if err = unmarshal(resp, &quoteResp); err != nil {
			return nil, nextPageToken, err
		}

		for symbol, q := range quoteResp.Quotes {
			quotes[symbol] = append(quotes[symbol], q...)
			received += len(q)
		}
		if quoteResp.NextPageToken == nil {
			nextPageToken = ""
			break
		}
		nextPageToken = *quoteResp.NextPageToken
		q.Set("page_token", *quoteResp.NextPageToken)
	}
	return quotes, nextPageToken, nil
}

// GetBarsPaginatedRequest contains optional parameters for getting bars in a paginated way
type GetBarsPaginatedRequest struct {
	GetBarsRequest
	// PageToken is the pagination token to continue from
	PageToken string
}

func (c *Client) setQueryBarPaginatedRequest(q url.Values, symbols []string, req GetBarsPaginatedRequest) {
	c.setBasePaginatedQuery(q, basePaginatedRequest{
		baseRequest: baseRequest{
			Symbols:  symbols,
			Start:    req.Start,
			End:      req.End,
			Feed:     req.Feed,
			AsOf:     req.AsOf,
			Currency: req.Currency,
		},
		PageToken: req.PageToken,
	})
	adjustment := Raw
	if req.Adjustment != "" {
		adjustment = req.Adjustment
	}
	q.Set("adjustment", string(adjustment))
	timeframe := OneDay
	if req.TimeFrame.N != 0 {
		timeframe = req.TimeFrame
	}
	q.Set("timeframe", timeframe.String())
}

// GetBarsPaginated returns bars for the given symbol, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetBarsPaginated(symbol string, req GetBarsPaginatedRequest) ([]Bar, string, error) {
	resp, nextPageToken, err := c.GetMultiBarsPaginated([]string{symbol}, req)
	if err != nil {
		return nil, nextPageToken, err
	}
	return resp[symbol], nextPageToken, nil
}

// GetMultiBarsPaginated returns bars for the given symbols, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetMultiBarsPaginated(symbols []string, req GetBarsPaginatedRequest) (map[string][]Bar, string, error) {
	bars := make(map[string][]Bar, len(symbols))

	u, err := url.Parse(fmt.Sprintf("%s/%s/bars", c.opts.BaseURL, stockPrefix))
	if err != nil {
		return nil, "", err
	}

	q := u.Query()
	c.setQueryBarPaginatedRequest(q, symbols, req)

	received := 0
	nextPageToken := req.PageToken
	for req.TotalLimit == 0 || received < req.TotalLimit {
		setQueryLimit(q, req.TotalLimit, req.PageLimit, received, v2MaxLimit)
		u.RawQuery = q.Encode()

		resp, err := c.get(u)
		if err != nil {
			return nil, nextPageToken, err
		}

		var barResp multiBarResponse
		if err = unmarshal(resp, &barResp); err != nil {
			return nil, nextPageToken, err
		}

		for symbol, b := range barResp.Bars {
			bars[symbol] = append(bars[symbol], b...)
			received += len(b)
		}
		if barResp.NextPageToken == nil {
			nextPageToken = ""
			break
		}
		nextPageToken = *barResp.NextPageToken
		q.Set("page_token", *barResp.NextPageToken)
	}
	return bars, nextPageToken, nil
}

// GetAuctionsPaginatedRequest contains optional parameters for getting auctions in a paginated way
type GetAuctionsPaginatedRequest struct {
	GetAuctionsRequest
	// PageToken is the pagination token to continue from
	PageToken string
}

// GetAuctionsPaginated returns auctions for the given symbol, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetAuctionsPaginated(symbol string, req GetAuctionsPaginatedRequest) ([]DailyAuctions, string, error) {
	resp, nextPageToken, err := c.GetMultiAuctionsPaginated([]string{symbol}, req)
	if err != nil {
		return nil, nextPageToken, err
	}
	return resp[symbol], nextPageToken, nil
}

// GetMultiAuctionsPaginated returns auctions for the given symbols, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetMultiAuctionsPaginated(
	symbols []string, req GetAuctionsPaginatedRequest,
) (map[string][]DailyAuctions, string, error) {
	u, err := url.Parse(fmt.Sprintf("%s/%s/auctions", c.opts.BaseURL, stockPrefix))
	if err != nil {
		return nil, "", err
	}

	q := u.Query()
	c.setBasePaginatedQuery(q, basePaginatedRequest{
		baseRequest: baseRequest{
			Symbols:  symbols,
			Start:    req.Start,
			End:      req.End,
			Feed:     "sip",
			AsOf:     req.AsOf,
			Currency: req.Currency,
		},
		PageToken: req.PageToken,
	})

	auctions := make(map[string][]DailyAuctions, len(symbols))
	received := 0
	nextPageToken := req.PageToken
	for req.TotalLimit == 0 || received < req.TotalLimit {
		setQueryLimit(q, req.TotalLimit, req.PageLimit, received, v2MaxLimit)
		u.RawQuery = q.Encode()

		resp, err := c.get(u)
		if err != nil {
			return nil, nextPageToken, err
		}

		var auctionsResp multiAuctionsResponse
		if err = unmarshal(resp, &auctionsResp); err != nil {
			return nil, nextPageToken, err
		}

		for symbol, a := range auctionsResp.Auctions {
			auctions[symbol] = append(auctions[symbol], a...)
			received += len(a)
		}
		if auctionsResp.NextPageToken == nil {
			nextPageToken = ""
			break
		}
		nextPageToken = *auctionsResp.NextPageToken
		q.Set("page_token", *auctionsResp.NextPageToken)
	}
	return auctions, nextPageToken, nil
}

func setCryptoBasePaginatedQuery(q url.Values, symbols []string, start, end time.Time, pageToken string) {
	setCryptoBaseQuery(q, symbols, start, end)
	if pageToken != "" {
		q.Set("page_token", pageToken)
	}
}

// GetCryptoTradesPaginatedRequest contains optional parameters for getting crypto trades in a paginated way
type GetCryptoTradesPaginatedRequest struct {
	GetCryptoTradesRequest
	// PageToken is the pagination token to continue from
	PageToken string
}

// GetCryptoTradesPaginated returns trades for the given crypto symbol, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetCryptoTradesPaginated(symbol string, req GetCryptoTradesPaginatedRequest) ([]CryptoTrade, string, error) {
	resp, nextPageToken, err := c.GetCryptoMultiTradesPaginated([]string{symbol}, req)
	if err != nil {
		return nil, nextPageToken, err
	}
	return resp[symbol], nextPageToken, nil
}

// GetCryptoMultiTradesPaginated returns trades for the given crypto symbols, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetCryptoMultiTradesPaginated(symbols []string, req GetCryptoTradesPaginatedRequest) (map[string][]CryptoTrade, string, error) {
	u, err := url.Parse(fmt.Sprintf("%s/%s/%s/trades", c.opts.BaseURL, cryptoPrefix, c.cryptoFeed(req.CryptoFeed)))
	if err != nil {
		return nil, "", err
	}

	q := u.Query()
	setCryptoBasePaginatedQuery(q, symbols, req.Start, req.End, req.PageToken)

	trades := make(map[string][]CryptoTrade, len(symbols))
	received := 0
	nextPageToken := req.PageToken
	for req.TotalLimit == 0 || received < req.TotalLimit {
		setQueryLimit(q, req.TotalLimit, req.PageLimit, received, v2MaxLimit)
		u.RawQuery = q.Encode()

		resp, err := c.get(u)
		if err != nil {
			return nil, nextPageToken, err
		}

		var tradeResp cryptoMultiTradeResponse
		if err = unmarshal(resp, &tradeResp); err != nil {
			return nil, nextPageToken, err
		}

		for symbol, t := range tradeResp.Trades {
			trades[symbol] = append(trades[symbol], t...)
			received += len(t)
		}
		if tradeResp.NextPageToken == nil {
			nextPageToken = ""
			break
		}
		nextPageToken = *tradeResp.NextPageToken
		q.Set("page_token", *tradeResp.NextPageToken)
	}
	return trades, nextPageToken, nil
}

// GetCryptoBarsPaginatedRequest contains optional parameters for getting crypto bars in a paginated way
type GetCryptoBarsPaginatedRequest struct {
	GetCryptoBarsRequest
	// PageToken is the pagination token to continue from
	PageToken string
}

func setQueryCryptoBarPaginatedRequest(q url.Values, symbols []string, req GetCryptoBarsPaginatedRequest) {
	setCryptoBasePaginatedQuery(q, symbols, req.Start, req.End, req.PageToken)
	timeframe := OneDay
	if req.TimeFrame.N != 0 {
		timeframe = req.TimeFrame
	}
	q.Set("timeframe", timeframe.String())
}

// GetCryptoBarsPaginated returns bars for the given crypto symbol, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetCryptoBarsPaginated(symbol string, req GetCryptoBarsPaginatedRequest) ([]CryptoBar, string, error) {
	resp, nextPageToken, err := c.GetCryptoMultiBarsPaginated([]string{symbol}, req)
	if err != nil {
		return nil, nextPageToken, err
	}
	return resp[symbol], nextPageToken, nil
}

// GetCryptoMultiBarsPaginated returns bars for the given crypto symbols, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetCryptoMultiBarsPaginated(symbols []string, req GetCryptoBarsPaginatedRequest) (map[string][]CryptoBar, string, error) {
	u, err := url.Parse(fmt.Sprintf("%s/%s/%s/bars",
		c.opts.BaseURL, cryptoPrefix, c.cryptoFeed(req.CryptoFeed)))
	if err != nil {
		return nil, "", err
	}

	q := u.Query()
	setQueryCryptoBarPaginatedRequest(q, symbols, req)

	bars := make(map[string][]CryptoBar, len(symbols))
	received := 0
	nextPageToken := req.PageToken
	for req.TotalLimit == 0 || received < req.TotalLimit {
		setQueryLimit(q, req.TotalLimit, req.PageLimit, received, v2MaxLimit)
		u.RawQuery = q.Encode()

		resp, err := c.get(u)
		if err != nil {
			return nil, nextPageToken, err
		}

		var barResp cryptoMultiBarResponse
		if err = unmarshal(resp, &barResp); err != nil {
			return nil, nextPageToken, err
		}

		for symbol, b := range barResp.Bars {
			bars[symbol] = append(bars[symbol], b...)
			received += len(b)
		}
		if barResp.NextPageToken == nil {
			nextPageToken = ""
			break
		}
		nextPageToken = *barResp.NextPageToken
		q.Set("page_token", *barResp.NextPageToken)
	}
	return bars, nextPageToken, nil
}

// GetNewsPaginatedRequest contains optional parameters for getting news articles in a paginated way.
type GetNewsPaginatedRequest struct {
	GetNewsRequest
	// PageToken is the pagination token to continue to next page
	PageToken string
}

func (c *Client) setNewsPaginatedQuery(q url.Values, p GetNewsPaginatedRequest) {
	c.setNewsQuery(q, p.GetNewsRequest)
	if p.PageToken != "" {
		q.Set("page_token", p.PageToken)
	}
}

// GetNewsPaginated returns the news articles based on the given req, and returns the nextPageToken to allow for manual pagination.
func (c *Client) GetNewsPaginated(req GetNewsPaginatedRequest) ([]News, string, error) {
	if req.TotalLimit < 0 {
		return nil, "", fmt.Errorf("negative total limit")
	}
	if req.PageLimit < 0 {
		return nil, "", fmt.Errorf("negative page limit")
	}
	if req.NoTotalLimit && req.TotalLimit != 0 {
		return nil, "", fmt.Errorf("both NoTotalLimit and non-zero TotalLimit specified")
	}
	u, err := url.Parse(fmt.Sprintf("%s/v1beta1/news", c.opts.BaseURL))
	if err != nil {
		return nil, "", fmt.Errorf("invalid news url: %w", err)
	}

	q := u.Query()
	c.setNewsPaginatedQuery(q, req)
	received := 0
	totalLimit := req.TotalLimit
	if req.TotalLimit == 0 && !req.NoTotalLimit {
		totalLimit = newsMaxLimit
	}

	nextPageToken := req.PageToken
	news := make([]News, 0, totalLimit)
	for totalLimit == 0 || received < totalLimit {
		setQueryLimit(q, totalLimit, req.PageLimit, received, newsMaxLimit)
		u.RawQuery = q.Encode()

		resp, err := c.get(u)
		if err != nil {
			return nil, nextPageToken, fmt.Errorf("failed to get news: %w", err)
		}

		var newsResp newsResponse
		if err = unmarshal(resp, &newsResp); err != nil {
			return nil, nextPageToken, fmt.Errorf("failed to unmarshal news: %w", err)
		}

		news = append(news, newsResp.News...)
		if newsResp.NextPageToken == nil {
			return news, "", nil
		}
		nextPageToken = *newsResp.NextPageToken
		q.Set("page_token", *newsResp.NextPageToken)
		received += len(newsResp.News)
	}
	return news, nextPageToken, nil
}
