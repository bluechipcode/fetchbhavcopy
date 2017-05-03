CREATE TABLE nse_eod_stock_data (
symbol character(20) NOT NULL,
series character(4) NOT NULL,
open numeric(30,2),
high numeric(30,2),
low numeric(30,2),
close numeric(30,2),
last numeric(30,2),
prevclose numeric(30,2),
tottrdqty numeric(30,0),
tottrdval numeric(30,2),
timestamp date NOT NULL,
delivqty numeric(30,0),

PRIMARY KEY (timestamp, symbol, series)
);

CREATE TABLE nse_eod_fo_data (
instrument text NOT NULL,
symbol text NOT NULL,
expiry_dt date NOT NULL,
strike_pr numeric(30,2) NOT NULL,
option_typ text NOT NULL,
open numeric(30,2),
high numeric(30,2),
low numeric(30,2),
close numeric(30,2),
settle_pr numeric(30,2),
contracts numeric(30,0),
val_inlakh numeric(30,2),
open_int numeric(30,0),
chg_in_oi numeric(30,0),
date date NOT NULL,

PRIMARY KEY (date, instrument, symbol, expiry_dt, strike_pr, option_typ)
);
