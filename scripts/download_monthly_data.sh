trade_date=2025-01
for symbol in BTCUSDT ETHUSDT SOLUSDT; do
  dir="data/monthly/$symbol"; file="$symbol-trades-$trade_date.zip"
  url="https://data.binance.vision/data/spot/monthly/trades/$symbol/$file"
  mkdir -p "$dir"
  ( cd "$dir"
    curl -fSL --retry 3 -O "$url" -O "$url.CHECKSUM"
    sha256sum -c "$file.CHECKSUM"
    unzip -o "$file" )
done