#!/bin/bash

set -e

CONFIG_FILE="./config/app_config.toml"
KEY_OUTPUT_PATH="./config/gcp-key.json"
SERVICE_ACCOUNT_NAME="airflow-runner"

# === Lire une valeur depuis app_config.toml ===
extract_toml_value() {
  SECTION=$1
  KEY=$2
  awk -v section="$SECTION" -v key="$KEY" '
    $0 ~ "\\["section"\\]" { in_section=1; next }
    /^\[/ { in_section=0 }
    in_section && $0 ~ key"[[:space:]]*=" {
      gsub(/"/, "", $3); print $3
    }
  ' "$CONFIG_FILE"
}

# === Lire la config TOML ===
PROJECT_ID=$(extract_toml_value gcp project_id)
DATASET=$(extract_toml_value gcp dataset)
REGION="EU"  # Ajustable si besoin

if [[ -z "$PROJECT_ID" || -z "$DATASET" ]]; then
  echo "❌ project_id ou dataset manquant dans config/app_config.toml"
  exit 1
fi

SERVICE_ACCOUNT_EMAIL="$SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"

# === Étape 1 : Créer compte de service et rôles ===
create_service_account() {
  echo "➡️ Création du compte : $SERVICE_ACCOUNT_NAME"
  gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
    --project="$PROJECT_ID" \
    --display-name="Service Account for Airflow" || echo "ℹ️ Déjà existant"

  echo "✅ Compte créé : $SERVICE_ACCOUNT_EMAIL"

  echo "➡️ Attribution des rôles..."
  ROLES=(
    roles/storage.admin
    roles/bigquery.dataEditor
    roles/bigquery.dataViewer
    roles/bigquery.jobUser
    roles/logging.logWriter
  )

  for ROLE in "${ROLES[@]}"; do
    echo "🔗 Rôle : $ROLE"
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
      --role="$ROLE" || true
  done

  echo "✅ Rôles attribués"

  echo "➡️ Création de la clé..."
  mkdir -p "$(dirname "$KEY_OUTPUT_PATH")"
  gcloud iam service-accounts keys create "$KEY_OUTPUT_PATH" \
    --iam-account="$SERVICE_ACCOUNT_EMAIL" \
    --project="$PROJECT_ID" || echo "ℹ️ Clé peut-être déjà créée"

  echo "✅ Clé générée : $KEY_OUTPUT_PATH"
}

# === Étape 2 : Créer dataset BigQuery ===
create_bigquery_dataset() {
  echo "➡️ Vérification dataset BigQuery : $DATASET"
  if bq --project_id="$PROJECT_ID" ls | grep -qw "$DATASET"; then
    echo "ℹ️ Dataset $DATASET déjà présent"
  else
    echo "📦 Création du dataset : $DATASET"
    bq --project_id="$PROJECT_ID" mk --location="$REGION" "$DATASET"
    echo "✅ Dataset créé"
  fi
}

# === Étape 3 : Lancer docker ===
start_airflow() {
  echo "🚀 Lancement d'Airflow..."
  sudo docker compose up -d --build
  echo "✅ Airflow prêt sur http://localhost:8080"
}

# === MAIN ===
create_service_account
create_bigquery_dataset
start_airflow
