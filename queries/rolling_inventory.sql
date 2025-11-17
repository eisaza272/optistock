-- Step 1: Generate the date range as a temp table
WITH temp_dates AS (
  SELECT day
  FROM UNNEST(
    GENERATE_DATE_ARRAY(DATE '2020-06-06', DATE '2025-08-06', INTERVAL 1 DAY)
  ) AS day
),

-- Step 2: Extract unique item ids as a temp table
temp_items AS (
  SELECT DISTINCT id
  FROM `optistockai.alegra.items_inventory`
),

-- Step 3: Cross join items with dates
temp_items_dates AS (
  SELECT 
    i.id,
    d.day
  FROM temp_items i
  CROSS JOIN temp_dates d
),

items_dates_ini_quantity as (
select dates.id, day, initial_quantity
from temp_items_dates dates
left join `optistockai.alegra.items_inventory` inventory ON dates.id = inventory.id and dates.day = inventory.initial_quantity_date
order by id, day)

#items_dates_ini_quantity as (
select dates.id, day, initial_quantity, quantity
from items_dates_ini_quantity dates
left join `optistockai.alegra.purchase_orders` purchase ON dates.id = purchase.item_id and dates.day = purchase.added_inventory_date
where quantity is not null

