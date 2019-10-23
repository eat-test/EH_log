

select    date(timestamp_seconds(cast(created_at as int64))) as datekey, count(*) as cnt
from      public.adjust_conversion
FOR SYSTEM TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 day)
where    timestamp_seconds(cast(created_at as int64)) >= '2019-08-01' and timestamp_seconds(cast(created_at as int64)) < timestamp_add('2019-10-21', interval 1 day)
group by 1
order by 1



/*현황 파악 - 가입당일 게임 미 진행 유저들의 가입 시간 분포*/
with sample as (
  select distinct nid
  from `sz_dw.f_max_campaign`
  where datekey = reg_datekey and reg_datekey<= '2019-10-01'
  and id_campaign = 0
  and last_tutorial ='tutorial not enter'
)

select extract(hour from _i_t) as Hour, extract(minute from _i_t) as min, count(distinct nid) as UU
from `public.common_register`
where nid in (select nid from sample )
and _i_t < timestamp('2019-10-02')
group by 1, 2
order by 1, 2

/*현황파악 - 가입 후 11001 스테이지 클리어까지 걸리는 시간*/
with sample as (
    select timestamp_diff(B._i_t, A._i_t, minute) as min_time, count(distinct A.nid) as UU
    from(
        select nid, _i_t
        from public.common_register
        where _i_t < timestamp('2019-10-02')
    ) as A
    INNER JOIN(
        select nid, _i_t
        from public.pve_campaign_result
        where campaign_data.id_campaign = 11001
        and first_clear = 1
    )as B
    On A.nid = B.nid and date(A._i_t) = date(B._i_t)
    group by min_time
)

select case when min_time < 10 then concat('0', cast(min_time as string))
              when min_time <= 100 then cast(min_time  as string)
              when min_time > 100 then '100분 초과' end as min_time
        , sum(UU) as UU
from sample
group by min_time
order by min_time


/*23시 54분 까지만 데이터를 활용하자*/

-- not enter not play
-- not enter play
-- play



-- 가입당일 게임 미 진행 유저들의 평균 복귀율
with sample as (
    select distinct reg_datekey, nid
    from sz_dw.f_max_campaign
    where datekey = reg_datekey and id_campaign = 0 and last_tutorial = 'tutorial not enter'
    and reg_datekey <='2019-10-01'
    and datekey<= '2019-10-01'
)
, num as (
    SELECT diff
    FROM UNNEST(GENERATE_ARRAY(1, 75)) AS diff
)
select TA.reg_datekey, not_play_UU, TA.diff
, coalesce(return_UU, 0) as return_UU, sum(return_UU) over(partition by TA.reg_datekey ORDER BY TA.diff ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as return_UU_cum
from(
    select TA.reg_datekey, not_play_UU, diff
    from(
        select reg_datekey, count(distinct nid) as not_play_UU
        from sample as A
        group by reg_datekey
    )as TA
    cross join num as TB
)as TA
LEFT JOIN(
    select reg_datekey, diff, count(distinct nid) as return_UU
    from(
        select A.reg_datekey, A.nid, date_diff(datekey,A.reg_datekey,DAY) as diff
        from sample as A
        INNER JOIN(
            select reg_datekey, nid, min(datekey) as datekey
            from sz_dw.f_max_campaign
            where reg_datekey <='2019-10-01' and datekey > reg_datekey and id_campaign > 0
            -- and date_diff(datekey, reg_datekey, day)<= 7
            group by reg_datekey, nid
        )as B
        On A.reg_datekey = B.reg_datekey and A.nid = B.nid
    )temp
    group by reg_datekey, diff
)TB
ON TA.reg_datekey = TB.reg_datekey and TA.diff = TB.diff
order by TA.reg_datekey, diff


/*D+0, D+1 PVE 진행 횟수 n_1, n_2*/
-- 당일 PVE 진행
with sample as (
    select distinct nid
    from sz_dw.f_max_campaign
    where reg_datekey = datekey and reg_datekey <= '2019-10-01'
    and id_campaign > 0
)
, sample_2 as (
    select A.nid ,cnt as  cnt_1
    FROM (
        select nid, count(distinct id_campaign) as cnt
        from sz_dw.f_pve_campaign_result
        where reg_datekey <= '2019-10-01' and datekey <= '2019-10-01' and id_campaign is not null
        and datekey = reg_datekey
        group by nid
    )as A
    INNER JOIN sample as B
    ON A.nid =B.nid
)
, sample_3 as (
    select A.nid , cnt as cnt_2
    FROM (
        select nid, count(distinct id_campaign) as cnt
        from sz_dw.f_pve_campaign_result
        where reg_datekey <= '2019-10-01' and datekey <= '2019-10-01' and id_campaign is not null
        and date_diff(datekey, reg_datekey, day) = 1
        group by nid
    )as A
    INNER JOIN sample as B
    ON A.nid =B.nid
)

select cnt_1, coalesce(cnt_2, 0) as cnt_2, count(distinct A.nid) as UU
from(
    select nid, cnt_1
    from sample_2
)as A
LEFT JOIN sample_3 as B ON A.nid = B.nid
group by cnt_1, cnt_2
order by cnt_1, cnt_2




-- 당일 PVE 미진행
with sample as (
    select distinct nid
    from sz_dw.f_max_campaign
    where reg_datekey = datekey and reg_datekey <= '2019-10-01'
    and id_campaign = 0 and last_tutorial = 'tutorial not enter'
)
, sample_2 as (
    select nid, diff, id_campaign, row_number() over(PARTITION BY nid, diff order by datekey) as rn, datekey
    from(
        select A.nid , diff, id_campaign, datekey, reg_datekey
        FROM (
            select nid, date_diff(datekey , reg_datekey, day) as diff, id_campaign, datekey, reg_datekey
            from sz_dw.f_pve_campaign_result
            where reg_datekey <= '2019-10-01' and datekey<= '2019-10-01'
        )as A
        INNER JOIN sample as B
        ON A.nid =B.nid
    )temp
)
, sample_3 as (
    select A.nid, id_campaign
    from(
        select distinct nid, datekey
        from sample_2
        where rn = 1
    )as A
    LEFT JOIN(
        select distinct nid, datekey, id_campaign
        from sample_2
        where rn = 2
    )as B
    ON A.nid = B.nid
    where date_diff(B.datekey, A.datekey, day ) = 1
)

select cnt_1, coalesce(cnt_2, 0) as cnt_2, count(distinct A.nid) as UU
from(
    select nid, count(distinct case when rn = 1 then id_campaign end) as cnt_1
    from sample_2
    group by nid
)as A
LEFT JOIN(
    select nid, count(distinct id_campaign) as cnt_2
    from sample_3
    group by nid
)as B
ON A.nid =B.nid
group by cnt_1, cnt_2
order by cnt_1, cnt_2;




/* 가설1 -  당일게임 진행여부에 로딩타임이 미치는 영향이 있을까?*/
with sample_1 as (
    -- 당일 미 진행 + 15일 이내 진입
    select A.reg_datekey,  A.nid, loading_time_ms
    from(
                select A.reg_datekey,  A.nid
                from(
                  select distinct nid, reg_datekey
                  from sz_dw.f_max_campaign
                  where reg_datekey = datekey and id_campaign = 0 and last_tutorial = 'tutorial not enter' and reg_datekey <= '2019-10-01' and reg_datekey>= '2019-08-26'
                ) as A
                INNER JOIN(
                    select distinct nid, reg_datekey
                    from sz_dw.f_max_campaign
                    where date_diff(datekey, reg_datekey, DAY) <= 15 and id_campaign > 0 and reg_datekey <= '2019-10-01' and reg_datekey>= '2019-08-26'
                )as B
                ON A.nid = B.nid and A.reg_datekey = B.reg_datekey
    )as A
    INNER JOIN(
              select nid, cast(loading_time_ms as int64)/1000 as loading_time_ms, reg_datekey
              from(
                    select nid, SPLIT(loading_time_ms, ';')[OFFSET(0)] as loading_time_ms, reg_datekey
                    from(
                      select nid, SPLIT(add_rsn, ',')[OFFSET(1)] as loading_time_ms, date(_i_t) as reg_datekey
                      , extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
                      from public.common_gamemoney
                      where ( rsn = 'session/login' or rsn = 'linegames/login' ) and add_rsn != 'UNDEFINED'
                      and _i_t < timestamp('2019-10-02')
                    ) etwe
                    where Hour<= 23 and min <= 54
                )weq
    )AS C
    ON A.nid = C.nid and A.reg_datekey = C.reg_datekey
)

, sample_3 as (
    -- 당일 미 진행 + 15일 이내 미진입
    select A.reg_datekey,  A.nid, loading_time_ms
    from(
      select nid, reg_datekey
      from sz_dw.f_max_campaign
      where reg_datekey = datekey and id_campaign = 0 and last_tutorial = 'tutorial not enter' and reg_datekey <= '2019-10-01' and reg_datekey>= '2019-08-26'
      group by nid, reg_datekey
    ) as A
    INNER JOIN(
        select nid, reg_datekey
        from sz_dw.f_max_campaign
        where date_diff(datekey, reg_datekey, DAY) <= 15 and id_campaign = 0   and reg_datekey <= '2019-10-01' and reg_datekey>= '2019-08-26'
        group by nid, reg_datekey
    )as B
    ON A.nid = B.nid and A.reg_datekey = B.reg_datekey
    INNER JOIN(
      select nid, cast(loading_time_ms as int64)/1000 as loading_time_ms, reg_datekey
      from(
            select nid, SPLIT(loading_time_ms, ';')[OFFSET(0)] as loading_time_ms, reg_datekey
            from(
              select nid, SPLIT(add_rsn, ',')[OFFSET(1)] as loading_time_ms, date(_i_t) as reg_datekey
              , extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
              from public.common_gamemoney
              where ( rsn = 'session/login' or rsn = 'linegames/login' ) and add_rsn != 'UNDEFINED'
              and _i_t < timestamp('2019-10-02')
            ) etwe
            where Hour<= 23 and min <= 54
        )weq
    )AS C
    ON A.nid = C.nid and A.reg_datekey = C.reg_datekey
)

, sample_2 as (
    -- 당일 진행 & 2일 연속 진행
    select A.reg_datekey, loading_time_ms, A.nid
    from(
            select TA.nid, TA.reg_datekey
            FROM (
                select nid, reg_datekey, id_campaign
                from sz_dw.f_max_campaign
                where reg_datekey = datekey and reg_datekey <= '2019-10-01' and id_campaign > 0 and reg_datekey>= '2019-08-26'
            )as TA
            INNER JOIN(
                select nid, reg_datekey, id_campaign
                from sz_dw.f_max_campaign
                where date_diff(datekey , reg_datekey , day)  = 1 and reg_datekey <= '2019-10-01' and id_campaign > 0 and reg_datekey>= '2019-08-26'
            )as TB
            ON TA.nid =TB.nid and TA.id_campaign < TB.id_campaign
    ) as A
    INNER JOIN(
      select nid, cast(loading_time_ms as int64)/1000 as loading_time_ms, reg_datekey
      from(
            select nid, SPLIT(loading_time_ms, ';')[OFFSET(0)] as loading_time_ms, reg_datekey
            from(
              select nid, SPLIT(add_rsn, ',')[OFFSET(1)] as loading_time_ms, date(_i_t) as reg_datekey
              , extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
              from public.common_gamemoney
              where ( rsn = 'session/login' or rsn = 'linegames/login' ) and add_rsn != 'UNDEFINED'
              and _i_t < timestamp('2019-10-02')
            ) etwe
            where Hour<= 23 and min <= 54
        )weq
    )AS B
    ON A.nid = B.nid and A.reg_datekey = B.reg_datekey
)

, sample_2_2 as (
    -- 당일 진행 & 2일 연속 진행 X
    select A.reg_datekey, loading_time_ms, A.nid
    from(
            select TA.nid, TA.reg_datekey
            FROM (
                select nid, reg_datekey, id_campaign
                from sz_dw.f_max_campaign
                where reg_datekey = datekey and reg_datekey <= '2019-10-01' and id_campaign > 0 and reg_datekey>= '2019-08-26'
            )as TA
            INNER JOIN(
                select nid, reg_datekey, id_campaign
                from sz_dw.f_max_campaign
                where date_diff(datekey , reg_datekey , day)  = 1 and reg_datekey <= '2019-10-01' and id_campaign > 0 and reg_datekey>= '2019-08-26'
            )as TB
            ON TA.nid =TB.nid and TA.id_campaign >= TB.id_campaign
    ) as A
    INNER JOIN(
      select nid, cast(loading_time_ms as int64)/1000 as loading_time_ms, reg_datekey
      from(
            select nid, SPLIT(loading_time_ms, ';')[OFFSET(0)] as loading_time_ms, reg_datekey
            from(
              select nid, SPLIT(add_rsn, ',')[OFFSET(1)] as loading_time_ms, date(_i_t) as reg_datekey
              , extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
              from public.common_gamemoney
              where ( rsn = 'session/login' or rsn = 'linegames/login' ) and add_rsn != 'UNDEFINED'
              and _i_t < timestamp('2019-10-02')
            ) etwe
            where Hour<= 23 and min <= 54
        )weq
    )AS B
    ON A.nid = B.nid and A.reg_datekey = B.reg_datekey
)


select A.reg_datekey, A.avg_loading_time
,coalesce(late_enter_UU, 0) as late_enter_UU ,  enter_2dayUU, enter_not_2_dayUU, coalesce(not_enter_UU, 0) as not_enter_UU
from(
    select reg_datekey
    ,case when  loading_time_ms <100 then 'a.under_100'
           when loading_time_ms < 200 then 'b.under_200'
           when loading_time_ms < 300 then 'c.under_300'
           when loading_time_ms < 400 then 'd.under_400'
           when loading_time_ms < 500 then 'e.under_500'
           when loading_time_ms < 600 then 'f.under_600'
           when loading_time_ms < 700 then 'g.under_700'
           when loading_time_ms < 800 then 'h.under_800'
           when loading_time_ms < 900 then 'i.under_900'
           when loading_time_ms < 1000 then 'j.under_1000' else 'k.over_1000' end as  avg_loading_time
    , count(distinct nid) as enter_2dayUU
    from sample_2
    group by reg_datekey, avg_loading_time
)as A
LEFT JOIN(
    select reg_datekey
    ,case when  loading_time_ms <100 then 'a.under_100'
           when loading_time_ms < 200 then 'b.under_200'
           when loading_time_ms < 300 then 'c.under_300'
           when loading_time_ms < 400 then 'd.under_400'
           when loading_time_ms < 500 then 'e.under_500'
           when loading_time_ms < 600 then 'f.under_600'
           when loading_time_ms < 700 then 'g.under_700'
           when loading_time_ms < 800 then 'h.under_800'
           when loading_time_ms < 900 then 'i.under_900'
           when loading_time_ms < 1000 then 'j.under_1000' else 'k.over_1000' end as  avg_loading_time
    , count(distinct nid) as late_enter_UU
    from sample_1
    group by reg_datekey, avg_loading_time
)as B
On A.reg_datekey = B.reg_datekey and A.avg_loading_time = B.avg_loading_time
LEFT JOIN(
    select reg_datekey
    ,case when  loading_time_ms <100 then 'a.under_100'
           when loading_time_ms < 200 then 'b.under_200'
           when loading_time_ms < 300 then 'c.under_300'
           when loading_time_ms < 400 then 'd.under_400'
           when loading_time_ms < 500 then 'e.under_500'
           when loading_time_ms < 600 then 'f.under_600'
           when loading_time_ms < 700 then 'g.under_700'
           when loading_time_ms < 800 then 'h.under_800'
           when loading_time_ms < 900 then 'i.under_900'
           when loading_time_ms < 1000 then 'j.under_1000' else 'k.over_1000' end as  avg_loading_time
    , count(distinct nid) as not_enter_UU
    from sample_3
    group by reg_datekey, avg_loading_time
)as C
On A.reg_datekey = C.reg_datekey and A.avg_loading_time = C.avg_loading_time
LEFT JOIN(
    select reg_datekey
    ,case when  loading_time_ms <100 then 'a.under_100'
           when loading_time_ms < 200 then 'b.under_200'
           when loading_time_ms < 300 then 'c.under_300'
           when loading_time_ms < 400 then 'd.under_400'
           when loading_time_ms < 500 then 'e.under_500'
           when loading_time_ms < 600 then 'f.under_600'
           when loading_time_ms < 700 then 'g.under_700'
           when loading_time_ms < 800 then 'h.under_800'
           when loading_time_ms < 900 then 'i.under_900'
           when loading_time_ms < 1000 then 'j.under_1000' else 'k.over_1000' end as  avg_loading_time
    , count(distinct nid) as enter_not_2_dayUU
    from sample_2_2
    group by reg_datekey, avg_loading_time
)as D
On A.reg_datekey = D.reg_datekey and A.avg_loading_time = D.avg_loading_time
order by A.reg_datekey, A.avg_loading_time






/*마케팅 채널별 - 별 효과가 없는 듯.*/
/* 가설2 -  마케팅 채널별 게임 미 진행 여부에 미치는 영향이 있을까?*/

-- network 별 유저
with sample as (
select distinct coalesce(network_name, 'Nontracking-Organic') as network_name
from sz_dw.f_user_map
where reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
)

select A.network_name, not_play_UU, play_UU, not_play_UU/ (play_UU+not_play_UU) as ratio
from sample as A
LEFT JOIN(
    select coalesce(network_name, 'Nontracking-Organic') as network_name, count(distinct A.nid) as not_play_UU
    from(
        select distinct nid
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and last_tutorial = 'tutorial not enter' and id_campaign = 0
        and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
    )as A
    INNER JOIN(
        select nid, network_name
        from sz_dw.f_user_map
        where nru = 1
        and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
    )as B
    On A.nid =B.nid
    group by network_name
)as B
ON A.network_name = B.network_name
LEFT JOIN(
    -- PVE 진행 유저
    select coalesce(network_name, 'Nontracking-Organic') as network_name, count(distinct A.nid) as play_UU
    from(
        select nid
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and id_campaign > 0
        and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
    )as A
    INNER JOIN(
        select nid, network_name
        from sz_dw.f_user_map
        where nru = 1
        and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
    )as B
    On A.nid =B.nid
    group by network_name
)as C
On A.network_name = C.network_name

-- network 별 유저 (전체 기간)
with sample as (
select distinct coalesce(network_name, 'Nontracking-Organic') as network_name
from sz_dw.f_user_map
)

select A.network_name, not_play_UU, play_UU, not_play_UU/ (play_UU+not_play_UU) as ratio
from sample as A
LEFT JOIN(
    select coalesce(B.network_name, 'Nontracking-Organic') as network_name, count(distinct A.nid) as not_play_UU
    from(
        select distinct nid
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and last_tutorial = 'tutorial not enter' and id_campaign = 0
    )as A
    INNER JOIN(
--         select nid, network_name
--         from sz_dw.f_user_map
--         where nru = 1
--         and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
        select A.nid,coalesce(B.network_name, 'Nontracking-Organic') as network_name
        from(
            select nid, extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
            from public.common_register
            where extract(hour from _i_t) <=23 and extract(minute from _i_t) <= 54
        ) as A
        LEFT JOIN(
            select distinct nid, network_name
            from public.adjust_conversion
        )as B
        ON A.nid =B.nid
    )as B
    On A.nid =B.nid
    group by B.network_name
)as B
ON A.network_name = B.network_name
LEFT JOIN(
    -- PVE 진행 유저
    select coalesce(B.network_name, 'Nontracking-Organic') as network_name, count(distinct A.nid) as play_UU
    from(
        select nid
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and id_campaign > 0
    )as A
    INNER JOIN(
--         select nid, network_name
--         from sz_dw.f_user_map
--         where nru = 1
--         and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
        select A.nid,coalesce(B.network_name, 'Nontracking-Organic') as network_name
        from(
            select nid, extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
            from public.common_register
            where extract(hour from _i_t) <=23 and extract(minute from _i_t) <= 54
        ) as A
        LEFT JOIN(
            select distinct nid, network_name
            from public.adjust_conversion
        )as B
        ON A.nid =B.nid
    )as B
    On A.nid =B.nid
    group by B.network_name
)as C
On A.network_name = C.network_name




/*네트워크별 평균 매출 - 특정 기간*/
with sample as (
select distinct coalesce(network_name, 'Nontracking-Organic') as network_name
from sz_dw.f_user_map
)

select A.network_name, not_play_revenue/ not_play_UU as not_play_revenue, play_revenue/play_UU as play_revenue
from sample as A
LEFT JOIN(
    select coalesce(B.network_name, 'Nontracking-Organic') as network_name, count(distinct A.nid) as not_play_UU, sum(daily_revenue) as not_play_revenue
    from(
        select distinct nid
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and last_tutorial = 'tutorial not enter' and id_campaign = 0
        and reg_datekey>= '2019-08-26' and reg_datekey<= '2019-10-09'
    )as A
    INNER JOIN(
--         select nid, network_name
--         from sz_dw.f_user_map
--         where nru = 1
--         and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
        select A.nid,coalesce(B.network_name, 'Nontracking-Organic') as network_name
        from(
            select nid, extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
            from public.common_register
            where extract(hour from _i_t) <=23 and extract(minute from _i_t) <= 54
            and _i_t >= timestamp('2019-08-26') and _i_t < timestamp('2019-10-10')
        ) as A
        LEFT JOIN(
            select distinct nid, coalesce(network_name, 'Nontracking-Organic') as network_name
            from public.adjust_conversion
        )as B
        ON A.nid =B.nid
    )as B
    On A.nid =B.nid
    INNER JOIN(
        select nid, coalesce(network_name, 'Nontracking-Organic') as network_name, sum(daily_revenue) as daily_revenue
        from sz_dw.f_user_map
        where reg_datekey>= '2019-08-26' and reg_datekey<= '2019-10-09'
        and datekey>= '2019-08-26' and datekey<= '2019-10-09'
        group by nid, network_name

    )as C
    On A.nid =C.nid
    group by B.network_name
)as B
ON A.network_name = B.network_name
LEFT JOIN(
    -- PVE 진행 유저
    select coalesce(B.network_name, 'Nontracking-Organic') as network_name, count(distinct A.nid) as play_UU, sum(daily_revenue) as play_revenue
    from(
        select nid
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and id_campaign > 0
        and reg_datekey>= '2019-08-26' and reg_datekey<= '2019-10-09'
    )as A
    INNER JOIN(
--         select nid, network_name
--         from sz_dw.f_user_map
--         where nru = 1
--         and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
        select A.nid,coalesce(B.network_name, 'Nontracking-Organic') as network_name
        from(
            select distinct nid, extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
            from public.common_register
            where extract(hour from _i_t) <=23 and extract(minute from _i_t) <= 54
            and _i_t >= timestamp('2019-08-26') and _i_t < timestamp('2019-10-10')
        ) as A
        LEFT JOIN(
            select distinct nid, coalesce(network_name, 'Nontracking-Organic') as network_name
            from public.adjust_conversion
        )as B
        ON A.nid =B.nid
    )as B
    On A.nid =B.nid
    INNER JOIN(
        select nid, coalesce(network_name, 'Nontracking-Organic') as network_name, sum(daily_revenue) as daily_revenue
        from sz_dw.f_user_map
        group by nid, network_name
    )as C
    On A.nid =C.nid
    group by B.network_name
)as C
On A.network_name = C.network_name

-- 전체 기간
with sample as (
select distinct coalesce(network_name, 'Nontracking-Organic') as network_name
from sz_dw.f_user_map
)

select A.network_name, not_play_revenue/ not_play_UU as not_play_revenue, play_revenue/play_UU as play_revenue
from sample as A
LEFT JOIN(
    select coalesce(B.network_name, 'Nontracking-Organic') as network_name, count(distinct A.nid) as not_play_UU, sum(daily_revenue) as not_play_revenue
    from(
        select distinct nid
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and last_tutorial = 'tutorial not enter' and id_campaign = 0
    )as A
    INNER JOIN(
--         select nid, network_name
--         from sz_dw.f_user_map
--         where nru = 1
--         and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
        select A.nid,coalesce(B.network_name, 'Nontracking-Organic') as network_name
        from(
            select nid, extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
            from public.common_register
            where extract(hour from _i_t) <=23 and extract(minute from _i_t) <= 54
        ) as A
        LEFT JOIN(
            select distinct nid, network_name
            from public.adjust_conversion
        )as B
        ON A.nid =B.nid
    )as B
    On A.nid =B.nid
    INNER JOIN(
        select nid, coalesce(network_name, 'Nontracking-Organic') as network_name, sum(daily_revenue) as daily_revenue
        from sz_dw.f_user_map
        group by nid, network_name
    )as C
    On A.nid =C.nid
    group by B.network_name
)as B
ON A.network_name = B.network_name
LEFT JOIN(
    -- PVE 진행 유저
    select coalesce(B.network_name, 'Nontracking-Organic') as network_name, count(distinct A.nid) as play_UU, sum(daily_revenue) as play_revenue
    from(
        select nid
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and id_campaign > 0
    )as A
    INNER JOIN(
--         select nid, network_name
--         from sz_dw.f_user_map
--         where nru = 1
--         and reg_datekey >= '2019-08-26' and reg_datekey <= '2019-10-09'
        select A.nid,coalesce(B.network_name, 'Nontracking-Organic') as network_name
        from(
            select distinct nid, extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
            from public.common_register
            where extract(hour from _i_t) <=23 and extract(minute from _i_t) <= 54
        ) as A
        LEFT JOIN(
            select distinct nid, coalesce(network_name, 'Nontracking-Organic') as network_name
            from public.adjust_conversion
        )as B
        ON A.nid =B.nid
    )as B
    On A.nid =B.nid
    INNER JOIN(
        select nid, coalesce(network_name, 'Nontracking-Organic') as network_name, sum(daily_revenue) as daily_revenue
        from sz_dw.f_user_map
        group by nid, network_name
    )as C
    On A.nid =C.nid
    group by B.network_name
)as C
On A.network_name = C.network_name





/*가입 일자별 0의 비율*/
select extract( week from A.reg_datekey) as reg_datekey, network_name, count(distinct A.nid) as reg_UU, count(distinct B.nid) as not_play_UU
from(
    select reg_datekey, network_name, nid
    from sz_dw.f_user_map
    where nru = 1
)as A
LEFT JOIN(
    select reg_datekey, nid
    from sz_dw.f_max_campaign
    where datekey = reg_datekey
    and last_tutorial = 'tutorial not enter'
)as B
On A.reg_datekey = B.reg_datekey and A.nid = B.nid
group by 1, network_name
order by 1, network_name



/*가입 채널별 매출액 누적 합*/
select TA.date_diff_reg, TA.network_name, sum(daily_revenue) over(PARTITION BY TA.network_name order by TA.date_diff_reg ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as daily_revenue
from(
    select date_diff_reg, network_name
    from(
        SELECT date_diff_reg
        FROM UNNEST(GENERATE_ARRAY(1, 81)) AS date_diff_reg
    )as A
    CROSS JOIN (
        select network_name
        from unnest(['non-tracking-organic','QooApp','Pre_registration_LMSnPush','Organic','Off-Facebook Installs','Naver_CafePlug','GG_Content_Influencers','Google Organic Search','Facebook Installs','Official_Channels','LINE_Internal','Brandpage','Naver_Brandsearch','Adwords UAC Installs']) as network_name
    )as B
)as TA
LEFT JOIN(
    select date_diff_reg, coalesce(network_name, 'non-tracking-organic') as network_name, daily_revenue
    from(
        select date_diff_reg, network_name, sum(daily_revenue) as daily_revenue
        from sz_dw.f_user_map
        group by date_diff_reg, network_name
    )temp
    where daily_revenue is not null
)as TB
ON TA.network_name =TB.network_name and TA.date_diff_reg = TB.date_diff_reg



select TA.date_diff_reg, TA.network_name, sum(daily_revenue) over(PARTITION BY TA.network_name order by TA.date_diff_reg ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as daily_revenue
from(
    select date_diff_reg, network_name
    from(
        SELECT date_diff_reg
        FROM UNNEST(GENERATE_ARRAY(1, 36)) AS date_diff_reg
    )as A
    CROSS JOIN (
        select network_name
        from unnest(['non-tracking-organic','QooApp','Pre_registration_LMSnPush','Organic','Off-Facebook Installs','Naver_CafePlug','GG_Content_Influencers','Google Organic Search','Facebook Installs','Official_Channels','LINE_Internal','Brandpage','Naver_Brandsearch','Adwords UAC Installs']) as network_name
    )as B
)as TA
LEFT JOIN(
    select date_diff_reg, coalesce(network_name, 'non-tracking-organic') as network_name, daily_revenue
    from(
        select date_diff_reg, network_name, sum(daily_revenue) as daily_revenue
        from sz_dw.f_user_map
        where reg_datekey >= '2019-09-01'
        group by date_diff_reg, network_name
    )temp
    where daily_revenue is not null
)as TB
ON TA.network_name =TB.network_name and TA.date_diff_reg = TB.date_diff_reg





/*가입당일 게임을 안했던 유저들은 나중에도 안할까?*/
-- 평균 3 ~ 5일뒤에 돌아온다. (최근에도 돌아오는 유저들이 있다.)
-- 가입 당일 게임을 안했다면, D+1, D+2 일자가 지날수록 들어오는 유저의 비율은 어떻게 될까?
-- 30일 정도면 들어올 유저들이 다 들어온거 같다.
/* P( D+1 | None ) */
-- 당일 게임을 진행한 유저
with sample as (
    select A.nid
    from(
        select distinct nid
        from sz_dw.f_max_campaign
        where reg_datekey = datekey and reg_datekey <= '2019-10-01'
        and id_campaign > 0
    )as A

)
, sample_2 as (
    select A.nid, id_campaign, row_number() over(PARTITION BY A.nid order by id_campaign) as rn
    from sz_dw.f_max_campaign as A
    INNER JOIN sample as B
    ON A.nid =B.nid
    where reg_datekey <= '2019-10-01'
)
, sample_3 as (
  select A.nid, diff_0, diff_1
  from(
            select nid, case when id_campaign > 12000 then '2월드' else cast(id_campaign as string) end as diff_0
            from sample_2
            where rn = 1
  )as A
  INNER JOIN(
            select nid, case when id_campaign > 12000 then '2월드' else cast(id_campaign as string) end as diff_1
            from sample_2
            where rn = 2
  )as B
  ON A.nid = B.nid
)
select diff_0, diff_1, count(distinct nid) as UU
from sample_3
group by 1, 2
order by 1, 2


-- 당일 게임을 진행하지 않았던 유저
with sample as (
    select A.nid
    from(
        select distinct nid
        from sz_dw.f_max_campaign
        where reg_datekey = datekey and reg_datekey <= '2019-10-01'
        and last_tutorial = 'tutorial not enter'
    )as A
    INNER JOIN(
        select nid, extract(hour from _i_t) as Hour, extract(minute from _i_t) as min
        from public.common_register
        where extract(hour from _i_t) <=23 and extract(minute from _i_t) <= 54
    )as B
    ON A.nid =B.nid
)
, sample_2 as (
    select A.nid, id_campaign, row_number() over(PARTITION BY A.nid order by id_campaign) as rn
    from sz_dw.f_max_campaign as A
    INNER JOIN sample as B
    ON A.nid =B.nid
    where reg_datekey <= '2019-10-01'
)
, sample_3 as (
  select A.nid, diff_0, diff_1
  from(
            select nid, case when id_campaign > 12000 then '2월드' else cast(id_campaign as string) end as diff_0
            from sample_2
            where rn = 2
  )as A
  INNER JOIN(
            select nid, case when id_campaign > 12000 then '2월드' else cast(id_campaign as string) end as diff_1
            from sample_2
            where rn = 3
  )as B
  ON A.nid = B.nid
)
select diff_0, diff_1, count(distinct nid) as UU
from sample_3
group by 1, 2
order by 1, 2










with sample as (
    select distinct reg_datekey, nid
    from sz_dw.f_max_campaign
    where datekey = reg_datekey and id_campaign = 0 and last_tutorial = 'tutorial not enter'
    and reg_datekey <='2019-10-09'
)

select TA.reg_datekey, not_play_UU, play_UU, avg_diff, avg_diff_max
from(
        select reg_datekey, count(distinct nid) as not_play_UU
        from sample as A
        group by reg_datekey
)as TA
LEFT JOIN(
    select A.reg_datekey, count(distinct A.nid) as play_UU
    , avg( date_diff(datekey, A.reg_datekey, DAY)) as avg_diff
    , max( date_diff(datekey, A.reg_datekey, DAY)) as avg_diff_max
    from (
        select reg_datekey, nid
        from sample
    )as A
    INNER JOIN(
        select reg_datekey, nid, min(datekey) as datekey
        from sz_dw.f_max_campaign
        where reg_datekey <='2019-10-09' and datekey > reg_datekey and id_campaign > 0
        group by reg_datekey, nid
    )as B
    ON A.reg_datekey = B.reg_datekey and A.nid = B.nid
    group by A.reg_datekey
)as TB
ON TA.reg_datekey = TB.reg_datekey








-- 이렇게 복귀한 유저들은 어디까지 갔을까?
-- 어디까지 가긴 갔다. 일반유저와 비교해서 많이 간건지 비교를 해야겠다.
-- 많이 갔으면? 특이한거고...
-- 많이 못갔으면? 첫날 게임 진행은 중요한 거고 --> 첫날 게임 진행의 원인?은 무엇인까?
with sample as (
    select distinct reg_datekey, nid
    from sz_dw.f_max_campaign
    where datekey = reg_datekey and id_campaign = 0 and last_tutorial = 'tutorial not enter'
    and reg_datekey <='2019-10-09' and datekey<= '2019-10-09'
)
select TA.reg_datekey
, case when id_campaign > 12000 then '2월드 진입' else cast(id_campaign as string) end as id_campaign
, case when id_campaign_max > 12000 then '2월드 진입' else cast(id_campaign_max as string) end as id_campaign_max, not_play_UU, UU
from(
    select reg_datekey, count(distinct nid) as not_play_UU
    from sample
    group by reg_datekey
)as TA
LEFT JOIN(
    select A.reg_datekey, id_campaign, id_campaign_max, count(distinct A.nid) as UU
    from sample as A
    INNER JOIN(
        select reg_datekey, nid, min(id_campaign ) as id_campaign, max(id_campaign ) as id_campaign_max
        from sz_dw.f_max_campaign
        where reg_datekey <='2019-10-09' and datekey > reg_datekey and id_campaign > 0
        and date_diff(datekey, reg_datekey, day) <= 7
        group by reg_datekey, nid
    )as B
    On A.reg_datekey = B.reg_datekey and A.nid = B.nid
    group by A.reg_datekey, id_campaign, id_campaign_max
)TB
ON TA.reg_datekey = TB.reg_datekey
order by TA.reg_datekey, id_campaign


/* 0은 국가별 영향이 있나?*/
select TA.reg_country, TA.reg_mon, not_enter_UU, reg_UU
from(
    select reg_country, reg_mon, count(distinct A.nid) as not_enter_UU
    from (
        select distinct nid, extract(month from reg_datekey) as reg_mon
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and last_tutorial ='tutorial not enter'
    )as A
    INNER JOIN (
        select distinct nid, reg_country
        from sz_dw.f_user_map
        where nru = 1
    )as B
    On A.nid =B.nid
    group by reg_country, reg_mon
)as TA
LEFT JOIN(
    select reg_country, extract(month from reg_datekey) as reg_mon, count(distinct nid) as reg_UU
    from sz_dw.f_user_map
    where nru = 1
    group by reg_country, reg_mon
)as TB
On TA.reg_country = TB.reg_country and TA.reg_mon = TB.reg_mon





/* 0은 로딩타임에 문제가 있나?*/
select
case when loading_time_ms < 100 then 'under_100'
      when loading_time_ms < 200 then 'under_200'
      when loading_time_ms < 300 then 'under_300'
      when loading_time_ms < 400 then 'under_400'
      when loading_time_ms < 500 then 'under_500'
      when loading_time_ms < 600 then 'under_600'
      when loading_time_ms < 700 then 'under_700'
      when loading_time_ms < 800 then 'under_800'
      else 'over 800' end as loading_time_ms
    , coalesce(reg_country, '기타') as reg_country, count(distinct TA.nid) as UU

from(
    select A.nid, loading_time_ms/1000 as loading_time_ms
    from(
        select distinct nid, reg_datekey
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and reg_datekey>='2019-09-11'
        and last_tutorial ='tutorial not enter'
    )as A
    INNER JOIN(
        select nid, loading_time_ms
        from(
            select nid, loading_time_ms, row_number() over(partition by nid ORDER BY _i_t) as rn
            from public.common_login
        )temp
        where rn = 1
    )as B
    ON A.nid =B.nid
)TA
LEFT JOIN(
        select nid, reg_country
        from sz_dw.f_user_map
        where reg_datekey >= '2019-09-11'
        and nru = 1
        and reg_country in ('BR','DE','ID','KR','TH','TW','UA','US','VN')
)as TC
On TA.nid = TC.nid
group by 1, 2





select
case when loading_time_ms < 100 then 'under_100'
      when loading_time_ms < 200 then 'under_200'
      when loading_time_ms < 300 then 'under_300'
      when loading_time_ms < 400 then 'under_400'
      when loading_time_ms < 500 then 'under_500'
      when loading_time_ms < 600 then 'under_600'
      when loading_time_ms < 700 then 'under_700'
      when loading_time_ms < 800 then 'under_800'
      else 'over 800' end as loading_time_ms
    , coalesce(reg_country, '기타') as reg_country, count(distinct TA.nid) as UU

from(
    select A.nid, loading_time_ms/1000 as loading_time_ms
    from(
        select distinct nid, reg_datekey
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and reg_datekey>='2019-09-11'
        and id_campaign > 0
    )as A
    INNER JOIN(
        select nid, loading_time_ms
        from(
            select nid, loading_time_ms, row_number() over(partition by nid ORDER BY _i_t) as rn
            from public.common_login
        )temp
        where rn = 1
    )as B
    ON A.nid =B.nid
)TA
LEFT JOIN(
        select nid, reg_country
        from sz_dw.f_user_map
        where reg_datekey >= '2019-09-11'
        and nru = 1
        and reg_country in ('BR','DE','ID','KR','TH','TW','UA','US','VN')
)as TC
On TA.nid = TC.nid
group by 1, 2





select
case when loading_time_ms < 100 then 'under_100'
      when loading_time_ms < 200 then 'under_200'
      when loading_time_ms < 300 then 'under_300'
      when loading_time_ms < 400 then 'under_400'
      when loading_time_ms < 500 then 'under_500'
      when loading_time_ms < 600 then 'under_600'
      when loading_time_ms < 700 then 'under_700'
      when loading_time_ms < 800 then 'under_800'
      else 'over 800' end as loading_time_ms
    , count(distinct TA.nid) as UU

from(
    select A.nid, loading_time_ms/1000 as loading_time_ms
    from(
        select distinct nid, reg_datekey
        from sz_dw.f_max_campaign
        where datekey = reg_datekey
        and id_campaign > 0
        and reg_datekey>='2019-09-11'
    )as A
    INNER JOIN(
        select nid, loading_time_ms
        from(
            select nid, loading_time_ms, row_number() over(partition by nid ORDER BY _i_t) as rn
            from public.common_login
        )temp
        where rn = 1
    )as B
    ON A.nid =B.nid
)TA
group by 1















/***************************************************************/

select AA.model, sum(install_cnt) as install_cnt, sum(register_cnt) as register_cnt, sum(login_UU) as login_UU
from sz_dw.dim_osv_temp as AA
LEFT JOIN(
        select dm, osv, count(distinct _id) as install_cnt
        from public.common_installed
        where _i_t < TIMESTAMP ('2019-10-10')
        group by dm, osv
    )as A
ON AA.dm = A.dm and AA.osv = A.osv
LEFT JOIN(
        select dm, osv, count(distinct nid) as register_cnt
        from public.common_register as A
        where _i_t < TIMESTAMP ('2019-10-10')
        group by dm, osv
)as B
ON AA.dm = B.dm and AA.osv = B.osv
LEFT JOIN(
        select dm, osv, count(distinct nid) as login_UU
        from public.common_login as A
        where _i_t < TIMESTAMP ('2019-10-10')
        group by dm, osv
        having count(distinct _id) > 1
)as C
ON AA.dm = C.dm and AA.osv = C.osv
group by 1