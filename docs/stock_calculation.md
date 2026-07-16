# Frame Stock Calculation - LTD

This note explains how frame stock consumption is calculated for Artesta Stores (UK) Ltd, meaning Proco frames.

In this document, `WACC` means the weighted average cost of stock per frame. It is also commonly written as `WAC`.

## Main Idea

The amount shown in the P&L as `Frame consumption` is not a new Proco invoice.

It is the value of the frames used during the month, calculated using the average cost of the stock available at the time.

In simple terms:

```text
frames consumed during the month x average cost per frame = stock consumption cost
```

That cost is posted to the P&L as part of manufacturing cost.

## Step By Step

### 1. Enter Frame Purchases

Before the monthly stock calculation can run, the Artesta team records each Proco frame purchase.

For each purchase, the team enters:

- the order date;
- the frame reference, meaning colour and size;
- the number of units purchased;
- the unit cost in `GBP`.

The unit cost includes:

- the cost of the frames;
- the transport cost.

The unit cost does not include:

- customs duties;
- import taxes;
- any separate customs-related cost.

This purchase information is what updates the average stock cost when new frames arrive.

### 2. Start With Opening Stock

At the start of the month we have:

- the number of frames in stock;
- the total value of that stock;
- the average cost per unit.

Example:

```text
Opening stock: 2,131 frames
Opening value: 19,745.79 GBP
Average opening cost: 19,745.79 / 2,131 = 9.27 GBP per frame
```

The real average cost is calculated by frame reference, meaning colour and size. Because of that, the final cost may not exactly match a simple global average.

### 3. Update The WACC When New Stock Is Purchased

When a new frame purchase arrives, the new units are added to stock and the WACC is recalculated.

Formula:

```text
new WACC =
(previous stock units x previous WACC + purchased units x purchase unit cost)
/ (previous stock units + purchased units)
```

Simple example:

```text
Stock before purchase: 100 frames at 8 GBP = 800 GBP
New purchase: 50 frames at 10 GBP = 500 GBP

New stock: 150 frames
New value: 1,300 GBP
New WACC: 1,300 / 150 = 8.67 GBP
```

Important points:

- WACC is calculated by frame reference, meaning colour and size.
- A white 30x40 frame has its own WACC.
- A black 50x70 frame has its own WACC.
- Consumption does not change the WACC. It only reduces the number of units in stock.
- Purchases change the WACC because they add new units at a new cost.

If a purchase arrives in the middle of the month, consumption before that date uses the old WACC, and consumption after that date uses the new WACC.

## Exchange Rate

For LTD, Proco stock is valued in `GBP`.

That means:

- the stock Excel is calculated in `GBP`;
- the LTD P&L line `Frame consumption` is also shown in `GBP`;
- no EUR exchange rate is needed inside the LTD stock calculation itself.

The exchange rate is only relevant when the group consolidated P&L converts LTD figures into the consolidated reporting currency.

The logic is:

```text
LTD frame consumption in GBP x applicable FX rate = consolidated value
```

So the stock calculation first produces the correct GBP cost. Any currency conversion happens afterwards at reporting/consolidation level.

### 4. Count The Frames Consumed During The Month

Each day, the system records how many frames have been used by frame reference.

For example:

```text
1 April: 3 frames
2 April: 2 frames
...
30 April: 16 frames
```

At the end of the month, all daily consumption is added up.

### 5. Apply The WACC Daily

The system does not wait until month-end and apply one single average to everything.

The WACC is applied daily.

Each day is valued as:

```text
frames consumed that day x WACC in force on that day
```

Example:

```text
Day 1:
3 frames x 9.27 GBP = 27.81 GBP

Day 2:
2 frames x 9.27 GBP = 18.54 GBP
```

If there were no purchases during the month, the WACC usually stays stable. If there were purchases, the WACC may change from the purchase date onwards.

Example with a mid-month purchase:

```text
1-14 April:
WACC = 8.00 GBP
Consumption during those days is valued at 8.00 GBP per frame.

15 April:
New stock purchase arrives.
WACC is recalculated to 8.67 GBP.

15-30 April:
Consumption from this point onwards is valued at 8.67 GBP per frame.
```

If a purchase and a consumption happen on the same day, the purchase is applied first. Therefore, that day's consumption uses the updated WACC.

### 6. Add Up All Daily Consumption

The monthly stock consumption cost is the sum of all valued daily consumption.

```text
monthly stock consumption cost =
sum of all daily valued consumption
```

This is the amount that goes into the LTD P&L line:

```text
Frame consumption
```

### 7. Calculate Closing Stock

Closing stock is calculated as:

```text
closing stock = opening stock + purchases - consumption
```

The closing stock value is the value of the frames still available at month-end.

## Real Example - April 2026 LTD

For Proco in April 2026:

```text
Opening stock: 2,131 frames
Opening value: 19,745.79 GBP
Purchases during the month: 0 frames
Frames consumed during the month: 244 frames
Stock consumption cost: 2,088.98 GBP
Closing stock: 1,887 frames
Closing stock value: 17,656.81 GBP
```

How to read it:

- April starts with `2,131` frames in stock.
- No new Proco frame purchases were added during April.
- `244` frames were consumed during the month.
- Those `244` frames were worth `2,088.98 GBP`, based on their frame type and average cost.
- Therefore, the LTD P&L shows `2,088.98 GBP` as `Frame consumption`.
- The month ends with `1,887` frames in stock.

Unit check:

```text
2,131 opening frames - 244 consumed frames = 1,887 closing frames
```

Value check:

```text
19,745.79 GBP opening value
- 2,088.98 GBP monthly consumption
= 17,656.81 GBP closing value
```

## What This Means In The P&L

In the LTD P&L, frame consumption is treated as a product cost.

The full stock purchase is not charged to the P&L when the stock is bought. Instead, the cost is charged gradually as frames are consumed.

This means:

- If we buy many frames but only consume a few, the P&L only shows the frames consumed.
- If we buy no frames in a month but use old stock, the P&L still shows the cost of the frames consumed.

This keeps the monthly gross margin aligned with the products actually sold and produced during the month.
