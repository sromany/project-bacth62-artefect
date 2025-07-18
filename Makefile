.PHONY: setup clean reset logs stop up build restart fix-perms

# Création du service account, rôles, clé GCP, dataset, etc.
setup:
	@echo "🚀 Setup complet GCP + Airflow"
	bash setup_airflow_gcp.sh

# Nettoyage total : containers, volumes, images, fichiers locaux
clean:
	@echo "🧹 Suppression des conteneurs, volumes, images, fichiers..."
	sudo docker compose down --volumes --remove-orphans
	sudo docker volume prune -f
	-@docker rmi $$(docker images 'apache/airflow*' -q) || true
	rm -rf dags/__pycache__ plugins/__pycache__ src/__pycache__
	rm -rf logs config/gcp-key.json

# Repart de zéro (clean + setup GCP)
reset: clean setup

# Démarrage classique (en arrière-plan)
up:
	@echo "🔼 Lancement des services Docker Airflow (mode détaché)"
	sudo docker compose up -d --build --force-recreate

# Redémarrage rapide (sans clean/setup)
restart:
	sudo docker compose down
	sudo docker compose up -d --build --force-recreate

# Redémarrage rapide Streamlit
restart-streamlit:
	sudo docker compose restart streamlit

# Redémarrage Streamlit avec rebuild
rebuild-streamlit:
	sudo docker compose up -d --build streamlit

# Affiche les logs Airflow en live
logs:
	sudo docker compose logs -f

# Stoppe les conteneurs (sans supprimer volumes)
stop:
	sudo docker compose down

# Fixe les permissions dans les conteneurs (utile si erreur Permission denied)
fix-perms:
	sudo chown -R 50000:0 logs config dags streamlit_app