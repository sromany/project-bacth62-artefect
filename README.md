Projet Data Engineering : Airflow + Streamlit + GCP
===================================================

Ce projet propose un pipeline complet pour traiter, stocker et visualiser des données météo et de consommation électrique, en exploitant :

- Apache Airflow (orchestration)
- Streamlit (visualisation interactive)
- Google Cloud Platform (BigQuery, Storage)

Prérequis
---------

- Accès à un projet Google Cloud avec droits Owner ou Editor
- Outils CLI installés sur la VM :
    - gcloud
    - bq
    - docker
    - docker compose
- Fichier de configuration à personnaliser :
    - config/app_config.toml (voir le .sample)
- Ouverture des ports sur la VM (si accès distant) :
    - 8501 (Streamlit)

Installation rapide
-------------------

1. Renseigner la configuration GCP  
   - Adapter config/app_config.toml avec votre project_id, dataset, etc.
   - Ne pas remplir config/gcp-key.json : le script la générera pour vous.
   - Penser à renseigner un nom unique pour le bucket.

2. Initialiser l’environnement et lancer la stack :
   make setup

   Ce script effectue :
   - Création du service account GCP, rôles et clé d’authentification
   - Création du dataset BigQuery
   - Lancement de tous les services Docker (Airflow, Streamlit…)

3. Accéder aux interfaces :
   - Airflow : http://localhost:8080 (login/pass : airflow / airflow)
   - Streamlit : http://localhost:8501
   - Flower : http://localhost:5555

   Si vous êtes sur une VM GCP, remplacez localhost par l’IP publique et ouvrez les ports nécessaires via les règles de pare-feu dans la console GCP.

Commandes Makefile utiles
-------------------------

- make setup    : Installation complète GCP + Docker
- make up       : Lancement de la stack Docker en mode détaché
- make restart  : Redémarrage rapide de la stack (sans clean/setup)
- make logs     : Afficher les logs Airflow en live
- make stop     : Arrêter tous les containers (sans supprimer les volumes)
- make clean    : Tout supprimer (containers, volumes, images, fichiers de config et logs)
- make reset    : Clean + setup (remise à zéro complète)
- make fix-perms: Corrige les permissions sur les dossiers utiles (si erreur “Permission denied”)

Déroulement du script d’installation (setup_airflow_gcp.sh)
------------------------------------------------------------

- Extraction automatique des variables du fichier config/app_config.toml
- Création du compte de service GCP et attribution des rôles nécessaires
- Génération de la clé d’authentification GCP dans config/gcp-key.json
- Création du dataset BigQuery
- Lancement de la stack Docker (Airflow, Streamlit…)

Ce script est idempotent : il ne plante pas si les éléments existent déjà.

Nettoyage et reset
------------------

Pour tout remettre à zéro :
    make reset

Cela efface les containers, volumes, images et fichiers temporaires.

Accès distant : ouverture des ports sur GCP
-------------------------------------------

Pour rendre l’application accessible publiquement :

- Aller dans Google Cloud Console > VPC > Règles de pare-feu
- Ajouter une règle autorisant le trafic entrant sur les ports :
    - 8501 (Streamlit)

Exemple avec gcloud CLI :

    gcloud compute firewall-rules create allow-streamlit --allow tcp:8501 --target-tags=YOUR-VM-TAG --source-ranges=0.0.0.0/0

Ou bien faites-le via l’interface GCP.

Sécurité
--------

- Ne publiez jamais votre config/gcp-key.json sur un dépôt public.
- Nettoyez régulièrement les clés inutilisées depuis la console GCP.

Contributions
-------------

Propositions de PR ou issues bienvenues.

License
-------

MIT License

