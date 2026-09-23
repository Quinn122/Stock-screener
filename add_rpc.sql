-- Run ONCE in the Supabase SQL editor (after the two tables exist).
-- Returns each symbol's latest N bars as ONE row, so the browser needs a
-- single request instead of paging through ~130,000 rows (the API caps
-- normal table reads at 1,000 rows per request).
create or replace function screener_get_bars(p_symbols text[], p_limit int default 260)
returns table (symbol text, bars jsonb)
language sql
stable
as $$
  select t.symbol,
         jsonb_agg(jsonb_build_array(t.date, t.open, t.high, t.low, t.close, t.volume)
                   order by t.date) as bars
  from (
    select b.*, row_number() over (partition by b.symbol order by b.date desc) as rn
    from screener_price_bars b
    where b.symbol = any(p_symbols)
  ) t
  where t.rn <= p_limit
  group by t.symbol;
$$;

grant execute on function screener_get_bars(text[], int) to anon;
