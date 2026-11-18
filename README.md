# Mini klaster Apache Spark na GCP

Projekt szkoleniowy DevOps — mini klaster **Apache Spark** na **Google Cloud Platform** zarządzany przez **Terraform**.

## Stack

| Warstwa | Technologia |
|---|---|
| IaC | Terraform >= 1.5, provider `hashicorp/google ~> 5.0` |
| Sieć | VPC, subnet, firewall (bez publicznych IP, IAP Tunnel) |
| Obliczenia | GCP Dataproc (Spark 3.5 / obraz `2.2-debian12`) |
| Pamięć | Cloud Storage (staging bucket + data bucket) |
| Obserwowalność | OpenTelemetry Collector → Managed Service for Prometheus + Cloud Trace, Grafana na Cloud Run |
| Region | `europe-central2` (Warszawa) |

## Architektura

```
┌───────────────────────────────────────┐
│             GCP Project               │
│  ┌────────────────────────────────┐   │
│  │             VPC                │   │
│  │  ┌──────────────────────────┐  │   │
│  │  │  Subnet 10.10.0.0/24    │  │   │
│  │  │                          │  │   │
│  │  │  ┌────────────────────┐  │  │   │
│  │  │  │  Dataproc Cluster  │  │  │   │
│  │  │  │  Master + 2 Worker │  │  │   │
│  │  │  └────────┬───────────┘  │  │   │
│  │  └───────────│──────────────┘  │   │
│  └──────────────│─────────────────┘   │
│                 │                     │
│  ┌──────────────▼─────────────────┐   │
│  │       Cloud Storage (GCS)      │   │
│  │  spark-staging-*  spark-data-* │   │
│  └────────────────────────────────┘   │
└───────────────────────────────────────┘
```

**Klaster w liczbach:**
- Master: `n1-standard-2` — 2 vCPU, 7.5 GB RAM
- 2x Worker: `n1-standard-2` — 2 vCPU, 7.5 GB RAM każdy
- Łącznie: 6 vCPU | ~22.5 GB RAM

## Struktura projektu

```
gcp_spark/
├── terraform/
│   ├── main.tf              ← root module (provider, API, łączy moduły)
│   ├── variables.tf         ← parametry wejściowe
│   ├── outputs.tf           ← wartości po apply (np. polecenie SSH tunnel)
│   ├── terraform.tfvars.example
│   └── modules/
│       ├── network/         ← VPC, subnet, firewall
│       ├── storage/         ← buckety GCS, service account
│       ├── dataproc/        ← klaster Spark
│       └── observability/   ← OpenTelemetry Collector, Grafana na Cloud Run
└── scripts/
    ├── word_count.py        ← przykładowy job PySpark
    └── submit_job.sh        ← skrypt do uruchomienia joba
```

## Wymagania wstępne

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.5
- [gcloud CLI](https://cloud.google.com/sdk/docs/install) z aktywną sesją (`gcloud auth application-default login`)
- Projekt GCP z włączonymi płatnościami
- Bucket GCS na backend Terraform state

## Szybki start

### 1. Konfiguracja zmiennych

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
# Edytuj terraform.tfvars — uzupełnij project_id
```

### 2. Inicjalizacja i wdrożenie

```bash
terraform init -backend-config="bucket=<TWOJ_BUCKET_NA_STATE>"
terraform plan
terraform apply
```

Klaster jest gotowy po ~2 minutach.

### 3. Uruchomienie przykładowego joba

```bash
# Pobierz nazwę data bucketu z outputów Terraform
DATA_BUCKET=$(terraform output -raw data_bucket)

cd ..
./scripts/submit_job.sh <PROJECT_ID> europe-central2 spark-training-cluster "$DATA_BUCKET"
```

Wyniki pojawią się w `gs://<data-bucket>/output/`.

### 4. Spark UI przez SSH tunnel

```bash
# Polecenie z outputu Terraform:
terraform output spark_ui_port_forward
```

- `localhost:4040` — Spark UI (joby, DAG, executory)
- `localhost:8080` — YARN ResourceManager

### 5. Grafana

Dostęp do Grafany chroni IAM Cloud Run (`roles/run.invoker`); sama Grafana nie pyta o hasło. Gotowe polecenie proxy jest w outpucie Terraform:

```bash
terraform output -raw grafana_proxy
# gcloud run services proxy grafana-dev \
#   --region=europe-central2 --project=<PROJECT_ID> --port=3000
```

Uruchom wypisane polecenie, a potem otwórz http://localhost:3000. Owner i Editor projektu mają dostęp od razu, pozostałych dopisz w zmiennej `grafana_invokers`. Wejście wprost na adres `*.run.app` kończy się błędem 403, bo przeglądarka nie wysyła tokenu.

Dashboard **Klaster Spark (Dataproc)** otwiera się jako strona startowa. Szczegóły w sekcji [Obserwowalność](#obserwowalność).

### 6. Usunięcie klastra

```bash
terraform destroy
```

> **Koszt:** klaster kosztuje ~$0.28/h nawet bezczynny. Konfiguracja zawiera `idle_delete_ttl = 3600s` — klaster wyłącza się automatycznie po 1h bez aktywności.

## Zmienne Terraform

| Zmienna | Domyślna | Opis |
|---|---|---|
| `project_id` | — | ID projektu GCP (wymagana) |
| `region` | `europe-central2` | Region GCP |
| `zone` | `europe-central2-a` | Strefa GCP |
| `env` | `dev` | Środowisko (dev / staging / prod) |
| `cluster_name` | `spark-training-cluster` | Nazwa klastra Dataproc |
| `num_workers` | `2` | Liczba węzłów worker |
| `master_machine` | `n1-standard-2` | Typ maszyny mastera |
| `worker_machine` | `n1-standard-2` | Typ maszyny workerów |
| `spark_version` | `2.2-debian12` | Obraz Dataproc (Spark 3.5, Hadoop 3.3) |
| `grafana_invokers` | `[]` | Dodatkowi członkowie IAM z dostępem do Grafany (np. `user:jan@example.com`) |

## Outputy Terraform

| Output | Opis |
|---|---|
| `cluster_name` | Nazwa klastra Dataproc |
| `master_instance_name` | Nazwa VM mastera |
| `staging_bucket` | Bucket GCS dla Dataproc |
| `data_bucket` | Bucket GCS na dane i wyniki |
| `spark_ui_port_forward` | Gotowe polecenie SSH tunnel do Spark UI |
| `grafana_url` | Adres Grafany na Cloud Run (wymaga uwierzytelnienia IAM) |
| `grafana_proxy` | Gotowe polecenie otwierające Grafanę na `localhost:3000` |

## Obserwowalność

```
węzły Dataproc                            poza klastrem (trwałe)
┌───────────────────────────┐
│ word_count.py ──OTLP──┐   │
│                       ▼   │          ┌────────────────────┐      ┌─────────────────────┐
│ hostmetrics ──▶ otelcol ──┼────────▶ │ Managed Prometheus │ ◀─── │ Grafana (Cloud Run) │
└───────────────────────────┘          │ + Cloud Trace      │      └─────────────────────┘
                                       └────────────────────┘
```

Całość jest w module `terraform/modules/observability`:

- **OpenTelemetry Collector** (`otelcol-contrib`, usługa systemd) działa na każdym węźle. Zbiera metryki maszyny (CPU, pamięć, dysk, sieć) i przyjmuje OTLP na `localhost:4317` / `4318`. Konfiguracja: `files/otel/collector-config.yaml`.
- **Metryki** trafiają do Google Cloud Managed Service for Prometheus, **ślady** do Cloud Trace. Ruch idzie przez Private Google Access, bez NAT.
- **Job `word_count.py`** wysyła do collectora spany (`word_count` → `count_words`, `save_output`) oraz metryki `wordcount.distinct_words` i `wordcount.duration`. Bez collectora albo bibliotek OpenTelemetry job działa normalnie, tylko bez telemetrii.
- **Grafana** działa na Cloud Run i skaluje się do zera (pierwsze wejście po przerwie trwa kilkanaście sekund). Datasource i dashboardy są w repo (`files/grafana/`) i montowane z bucketu GCS — zmiany wyklikane w Grafanie znikają przy restarcie, więc trwałe zmiany rób w plikach.

Jak otworzyć Grafanę i kto ma do niej dostęp: [5. Grafana](#5-grafana).

Węzły nie mają wyjścia do internetu, więc paczkę collectora i biblioteki Pythona pobiera maszyna wykonująca `terraform apply` (skrypt `files/otel/upload_artifacts.sh`, wymaga `curl`, `python3` z `pip` i `gcloud`) i wgrywa je do bucketu data. Init action instaluje je stamtąd przy tworzeniu klastra. Błąd instalacji nie blokuje klastra — szukaj wtedy wpisów `[otel]` w logu init action w buckecie staging.

Wersje (collector, biblioteki OpenTelemetry, obraz Grafany) są przypięte w `locals` modułu. Zmiana konfiguracji collectora trafia na węzły dopiero przy następnym utworzeniu klastra.

## CI/CD

Projekt zawiera przykład pipeline GitHub Actions (`github_actions_prezentacja.md`). Backend Terraform state na GCS — bucket i prefix przekazywane przez `-backend-config` w CI.

### Wykrywanie driftu

Workflow `.github/workflows/drift.yml` codziennie (i na żądanie z zakładki Actions) porównuje state Terraforma z rzeczywistym stanem w GCP (`terraform plan -refresh-only`). Gdy znajdzie różnice, otwiera lub aktualizuje issue z etykietą `drift`; gdy różnice znikną, zamyka je. Niczego nie naprawia sam:

- zmiana w GCP była pomyłką — uruchom `terraform apply`, żeby przywrócić stan z kodu,
- zmiana w GCP była celowa — przenieś ją do plików `.tf` zwykłym PR-em.

Usunięcie klastra przez `idle_delete_ttl` jest traktowane jako stan oczekiwany i nie otwiera issue.

## Materiały szkoleniowe

- `prezentacja.md` — slajdy szkolenia: Terraform, GCP, architektura Spark, demo
- `prezentacja_gcloud_login.md` — krok po kroku: logowanie gcloud
- `prezentacja_iac_claude.md` — IaC z pomocą AI
