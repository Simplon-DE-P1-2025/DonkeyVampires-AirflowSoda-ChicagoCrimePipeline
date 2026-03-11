DonkeyVampires-AirflowSoda-ChicagoCrimePipeline
dev

docker exec -it astro-project_5ea1c3-postgres_data-1 psql -U user -d chicago_data -c "SELECT primary_type, COUNT(*) as total_crimes, SUM(CASE WHEN arrest THEN 1 ELSE 0 END) as arrests FROM final_crimes GROUP BY primary_type ORDER BY total_crimes DESC LIMIT 10;"

 BATTERY             |         3731 |     659
 CRIMINAL DAMAGE     |         2058 |      81
 ASSAULT             |         1829 |     220
 MOTOR VEHICLE THEFT |         1617 |      51
 OTHER OFFENSE       |         1486 |     249
 DECEPTIVE PRACTICE  |         1122 |      34
 BURGLARY            |          955 |      23
 NARCOTICS           |          677 |     635
 CRIMINAL TRESPASS   |          520 |     181
(10 rows)