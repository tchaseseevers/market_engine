INSERT INTO agg_trade (
    event_type   ,
    event_time   , 
    symbol       ,
    aggregate_trade_id    ,
    price        ,
    quantity     ,
    first_trade_id        ,
    last_trade_id,
    trade_time   , 
    is_the_buyer_the_market_maker  , 
    recv_unix    ,
    recv_iso     
    )
SELECT 
    event_type   ,
    event_time   , 
    symbol       ,
    aggregate_trade_id    ,
    price        ,
    quantity     ,
    first_trade_id        ,
    last_trade_id,
    trade_time   , 
    is_the_buyer_the_market_maker  , 
    recv_unix    ,
    recv_iso     
FROM stage_agg_trade;



INSERT INTO trade (
    event_type   ,
    event_time   ,
    symbol       ,
    trade_id     ,
    price        ,
    quantity     ,
    buyer_order_id        ,
    seller_order_id       ,
    trade_time   ,
    is_the_buyer_the_market_maker  ,
    recv_unix    ,
    recv_iso     
    )
SELECT 
    event_type   ,
    event_time   ,
    symbol       ,
    trade_id     ,
    price        ,
    quantity     ,
    buyer_order_id        ,
    seller_order_id       ,
    trade_time   ,
    is_the_buyer_the_market_maker  ,
    recv_unix    ,
    recv_iso     
FROM stage_trade;



INSERT INTO klines1 (
    event_type    ,
    event_time    ,
    symbol        ,
    kline_start_time       ,
    kline_close_time       ,
    symbol2       ,
    interval      ,      -- "1m"
    first_trade_id,
    last_trade_id ,
    open_price    ,
    close_price   ,
    high_price    ,
    low_price     ,
    base_asset_volume      ,
    number_of_trades       ,
    is_this_kline_closed   ,   -- "True"/"False"
    quote_asset_volume     ,
    taker_buy_base_asset_volume     ,
    taker_buy_quote_asset_volume    ,
    recv_unix     ,
    recv_iso      
    )
SELECT 
    event_type    ,
    event_time    ,
    symbol        ,
    kline_start_time       ,
    kline_close_time       ,
    symbol2       ,
    interval      ,      -- "1m"
    first_trade_id,
    last_trade_id ,
    open_price    ,
    close_price   ,
    high_price    ,
    low_price     ,
    base_asset_volume      ,
    number_of_trades       ,
    is_this_kline_closed   ,   -- "True"/"False"
    quote_asset_volume     ,
    taker_buy_base_asset_volume     ,
    taker_buy_quote_asset_volume    ,
    recv_unix     ,
    recv_iso      
FROM stage_klines1;


INSERT INTO klines3 (
    event_type    ,
    event_time    ,
    symbol        ,
    kline_start_time       ,
    kline_close_time       ,
    symbol2       ,
    interval      ,      -- "1m"
    first_trade_id,
    last_trade_id ,
    open_price    ,
    close_price   ,
    high_price    ,
    low_price     ,
    base_asset_volume      ,
    number_of_trades       ,
    is_this_kline_closed   ,   -- "True"/"False"
    quote_asset_volume     ,
    taker_buy_base_asset_volume     ,
    taker_buy_quote_asset_volume    ,
    recv_unix     ,
    recv_iso      
    )
SELECT 
    event_type    ,
    event_time    ,
    symbol        ,
    kline_start_time       ,
    kline_close_time       ,
    symbol2       ,
    interval      ,      -- "1m"
    first_trade_id,
    last_trade_id ,
    open_price    ,
    close_price   ,
    high_price    ,
    low_price     ,
    base_asset_volume      ,
    number_of_trades       ,
    is_this_kline_closed   ,   -- "True"/"False"
    quote_asset_volume     ,
    taker_buy_base_asset_volume     ,
    taker_buy_quote_asset_volume    ,
    recv_unix     ,
    recv_iso      
FROM stage_klines3;



INSERT INTO klines5 (
    event_type    ,
    event_time    ,
    symbol        ,
    kline_start_time       ,
    kline_close_time       ,
    symbol2       ,
    interval      ,      -- "1m"
    first_trade_id,
    last_trade_id ,
    open_price    ,
    close_price   ,
    high_price    ,
    low_price     ,
    base_asset_volume      ,
    number_of_trades       ,
    is_this_kline_closed   ,   -- "True"/"False"
    quote_asset_volume     ,
    taker_buy_base_asset_volume     ,
    taker_buy_quote_asset_volume    ,
    recv_unix     ,
    recv_iso      
    )
SELECT 
    event_type    ,
    event_time    ,
    symbol        ,
    kline_start_time       ,
    kline_close_time       ,
    symbol2       ,
    interval      ,      -- "1m"
    first_trade_id,
    last_trade_id ,
    open_price    ,
    close_price   ,
    high_price    ,
    low_price     ,
    base_asset_volume      ,
    number_of_trades       ,
    is_this_kline_closed   ,   -- "True"/"False"
    quote_asset_volume     ,
    taker_buy_base_asset_volume     ,
    taker_buy_quote_asset_volume    ,
    recv_unix     ,
    recv_iso      
FROM stage_klines5;


INSERT INTO ticker (
    event_type     ,
    event_time     ,
    symbol,
    price_change   ,
    price_change_percent    ,
    weighted_average_price  ,
    prev_close_price        ,
    last_price     ,
    last_quantity  ,
    best_bid_price ,
    best_bid_quantity       ,
    best_ask_price ,
    best_ask_quantity       ,
    open_price     ,
    high_price     ,
    low_price      ,
    total_traded_base_asset_volume   ,
    total_traded_quote_asset_volume  ,
    statistics_open_time    ,
    statistics_close_time   ,
    first_trade_id ,
    last_trade_id  ,
    total_number_of_trades  ,
    recv_unix      ,
    recv_iso       
    )
SELECT 
    event_type     ,
    event_time     ,
    symbol,
    price_change   ,
    price_change_percent    ,
    weighted_average_price  ,
    prev_close_price        ,
    last_price     ,
    last_quantity  ,
    best_bid_price ,
    best_bid_quantity       ,
    best_ask_price ,
    best_ask_quantity       ,
    open_price     ,
    high_price     ,
    low_price      ,
    total_traded_base_asset_volume   ,
    total_traded_quote_asset_volume  ,
    statistics_open_time    ,
    statistics_close_time   ,
    first_trade_id ,
    last_trade_id  ,
    total_number_of_trades  ,
    recv_unix      ,
    recv_iso       
FROM stage_ticker;


INSERT INTO bookTicker (
    order_book_update_id   ,
    symbol        ,
    best_bid_price,
    best_bid_qty  ,
    best_ask_price,
    best_ask_qty  ,
    recv_unix     ,
    recv_iso      
    )
SELECT 
    order_book_update_id   ,
    symbol        ,
    best_bid_price,
    best_bid_qty  ,
    best_ask_price,
    best_ask_qty  ,
    recv_unix     ,
    recv_iso      
FROM stage_bookTicker;





INSERT INTO events (
    event_type        ,
    event_time        ,
    symbol    ,
    first_update_id   ,
    final_update_id   ,
    price    ,
    qty      ,
    side     ,
    recv_unix,
    recv_iso 
    )
SELECT 
    event_type        ,
    event_time        ,
    symbol    ,
    first_update_id   ,
    final_update_id   ,
    price    ,
    qty      ,
    side      ,
    recv_unix,
    recv_iso 
FROM stage_events;