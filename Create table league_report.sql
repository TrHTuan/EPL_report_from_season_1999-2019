use epl_1;
drop table if exists league_report;
create table league_report
(	team varchar(100),
    total_points varchar(3),
    champion_year varchar(4)
    );

select * from league_report;

drop procedure if exists insert_loop;
create procedure insert_loop()
insert into league_report(team, total_points,champion_year)
select L1.*
from
(
	select S1.team, S1.point_1 + S1.point_2 as total_points, S1.`year` as champion_year
	from
		(select A1.Team_1 as team, A1.point_1, B1.point_2, C1.`year`
		from
		(select distinct Team_1, sum(Team_1_score) as point_1
		from season_1999
		group by Team_1
		order by Team_1) as A1
		join
		(select distinct Team_2, sum(Team_2_score) as point_2
		from season_1999
		group by Team_2
		order by Team_2) as B1
		on A1.Team_1 = B1.Team_2
		cross join
		(select max(year) as year from season_1999) as C1
		) as S1
)as L1;