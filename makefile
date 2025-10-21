airflow-up:
	docker compose up -d --build

airflow-inyeccion:
	@echo "🚀 Iniciando inyección de dependencias en Airflow..."
	@ansible-playbook -i ansible/inventories/localhost.ini ansible/playbooks/airflow.yml --force-handlers
	@echo "✅ Inyección completada correctamente."

airflow-password:
	docker exec -it airflow bash -lc "cat simple_auth_manager_passwords.json.generated"