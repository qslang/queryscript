select 1 as f"foo_{s}";
select f"{ss}" from (select 1 as "s"); -- should be the value 1
select f'{ss}'; -- should be the value s

select
    for item in slices {
        item AS f"metric_{item}"
    }
;
