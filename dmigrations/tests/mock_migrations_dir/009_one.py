from gcap.apps.dmigrations import migrations as m

migration = m.Migration(sql_up="INSERT INTO mock VALUES (9, 1)", sql_down="")
