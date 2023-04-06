from plugins.alert_d2.factories import DagFactory

DagFactory.new(dag_id='alert_installs',
               #schema_path='dags/service_dags/sql_installs/installs.yaml',
               schema_path='sql_installs/installs.yaml',
               sql_path='sql_installs',
               schedule_interval='0 7,9 * * *')
