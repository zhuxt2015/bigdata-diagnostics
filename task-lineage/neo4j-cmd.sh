echo "清空neo4j数据库"
docker exec  985077b033cc /var/lib/neo4j/bin/cypher-shell 'match (n) detach delete n'
echo "导入task信息"
docker exec  985077b033cc /var/lib/neo4j/bin/cypher-shell "load csv with headers from 'file:/task.csv' as line FIELDTERMINATOR '#' with line create (:task {project_id:line.project_id,  project_name:line.project_name,  process_id:line.process_id,  process_name:line.process_name, task_id:line.task_id, task_name:line.task_name, task_type:line.task_type});"
echo "导入工作流信息"
docker exec  985077b033cc /var/lib/neo4j/bin/cypher-shell "load csv with headers from 'file:/process.csv' as line with line create (:process {project_id:line.project_id,  project_name:line.project_name,  process_id:line.process_id,  process_name:line.process_name});"
echo "导入工作流和任务之间包含关系"
docker exec  985077b033cc /var/lib/neo4j/bin/cypher-shell "load csv with headers from 'file:/process_task.csv' as line match (from:process {process_id:line.process_id}),(to:task {task_id:line.task_id}) merge (from)-[c:\u5305\u542b{relation:line.relation}]-(to)"
echo "导入任务之间依赖关系"
docker exec  985077b033cc /var/lib/neo4j/bin/cypher-shell "load csv with headers from 'file:/task_task.csv' as line match (from:task {task_id:line.task_id}),(to:task {task_id:line.pre_task_id}) merge (from)-[c:\u4f9d\u8d56{relation:line.relation}]-(to)"
echo "导入任务和工作流之间依赖关系"
docker exec  985077b033cc /var/lib/neo4j/bin/cypher-shell "load csv with headers from 'file:/task_process.csv' as line match (from:task {task_id:line.task_id}),(to:process {process_id:line.process_id}) merge (from)-[c:\u4f9d\u8d56{relation:line.relation}]-(to)"
echo "导入工作流与工作流之间依赖关系"
docker exec  985077b033cc /var/lib/neo4j/bin/cypher-shell "load csv with headers from 'file:/process_process.csv' as line match (from:process {process_id:line.process_id}),(to:process {process_id:line.process_id}) merge (from)-[c:\u4f9d\u8d56{relation:line.relation}]-(to)"
