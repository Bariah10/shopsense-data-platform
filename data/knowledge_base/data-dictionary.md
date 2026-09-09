# Order Data Dictionary

The orders stream carries one event per order state change on the Kafka topic orders.raw.
order_id is the business key and is unique per order. customer_id identifies the buyer.
quantity is a positive integer, unit_price is the price of one unit in the order currency,
and line_total is quantity multiplied by unit_price, computed in the Silver layer.
status is one of created, paid, shipped, delivered or cancelled.
Only paid, shipped and delivered orders count as revenue; created and cancelled orders do not.
order_ts is the moment the order was placed, delivered_ts is the moment it was handed to the customer,
and delivered_ts is never earlier than order_ts.
