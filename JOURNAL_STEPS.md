# pypipex
A TDD core project to develop Apache Beam with Python SDK

# Current Steps to create this repo

1. Create Venv
    ```
    # Create Venv
    python -m venv /path/to/directory

    # Activate venv
    . /path/to/directory/bin/activate

    source .venv/bin/activate
    ```
2. Full list installs applied
    ```
    # Install Apache Beam for local
    pip install apache-beam

    # Install Apache Beam for GCP
    pip install wheel
    pip install 'apache-beam[gcp]'
    pip install --upgrade 'apache-beam[gcp]'
    ```

3. Created requirements.txt from current venv
    ```
    # Create Requirements
    pip freeze > requirements.txt
    ```

4. Install requirements.txt
    ```
    # Install Requirements
    pip install -r requirements.txt
    ```

5. Example: to Run wordcount_minimal.py with DirectRunner
    From Examples:https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples
    - Word count minimal: https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount_minimal.py
    - Word count minimal test: https://github.com/apache/beam/blob/master/sdks/python/apache_beam/examples/wordcount_minimal_test.py

    ```
    # Run Direct Runner
    python wordcount_minimal.py --runner DirectRunner --input ./test/input/ch1_les_miserables.txt --output ./test/output/word_counts_ch1_les_miserables.txt

    # Run bash for the previus command
    bash run_wordcount_minimal_ch1_les_miserables.sh
    ```
    
    5.1 Run bash Tests: 
    ```
    bash run_test_wordcount_minimal.sh 
    ```
    or equivalent:
    ```
    #!/bin/bash
    python -m unittest tests.wordcount_tests.wordcount_minimal_test
    ```

    5.2 Run bash example :

    5.2.1
    For wordcount minimal Python Apache Beam Pipeline at DirectRunner, from /src/modules/wordcount_minimal.py

    ```
    bash run_wordcount_minimal_app_example.sh
    ```

    5.2.2 or equivalent:
    ```
    #!/bin/bash
    python3 -m src.pypipex.modules.wordcount_minimal \
    --runner DirectRunner \
    --input ./src/pypipex/tests/input/ch1_les_miserables.txt \
    --output ./src/pypipex/tests/output/word_counts_ch1_les_miserables_test_refactored_project.txt
    ```

    5.2.3 or equivalent when install pip install pypipex: Direct Runnner
    ```
    python -m src.modules.wordcount_minimal --runner DirectRunner \
    --input "gs://data-core-project-landing-zone/data_lake_core_web_scrapper/raw_cl_house_prices/2023-03-08 Precios Casas RM.csv" \
    --output "gs://data-core-project-landing-zone/data_lake_core_web_scrapper/raw_cl_house_prices_word_counts/2023-03-08-precios-casas-rm.txt"
    ```

    5.2.3 or equivalent when install pip install pypipex DataFlowRunner: 
    ```
    # WIP testing command to deploy job at DataflowRunner
    python -m src.modules.wordcount_minimal \
    --runner DataflowRunner \
    --job_name "wordcount-cl-house-prices" \
    --input "gs://data-core-project-landing-zone/data_lake_core_web_scrapper/raw_cl_house_prices/2023-03-08 Precios Casas RM.csv" \
    --output "gs://data-core-project-landing-zone/data_lake_core_web_scrapper/raw_cl_house_prices_word_counts/20230308_precios_casas_rm_dataflow_test.txt" \
    --project "israndroid-data-core-project" \
    --region "us-central1" \
    --staging_location "gs://data-core-project-landing-zone/data_lake_core_web_scrapper/staging" \
    --temp_location "gs://data-core-project-landing-zone/temp_beam"
    ```

    5.3 Run bash cleaning python cache files

    ```
    bash run_clean_cache.sh
    ```

    or equivalent: 
    ```
    #!/bin/bash
    find . | grep -E "(/__pycache__$|\.pyc$|\.pyo$)" | xargs rm -rf
    ```
 

6. Config base project (app, src, test ... setup.py, etc.).

    6.1 Local Flow: Will be able to run apache beam Direct Runner jobs. The base project will be can run Apache Beam pipelines in local with DirectRunner, and also if needed test local Airflow Dags in local env (or an Dockerized Dev Environment).

    6.2 The Cloud aproach (GCP): Using Airflow at Compute Engine, run Apache Beam Pipelines triggered by Airflow in a preemptible vm.

    Reference Doc for deploy in GCP:
    - Compute Engine:
    - Airflow in GCP: https://cloud.google.com/blog/products/data-analytics/different-ways-to-run-apache-airflow-on-google-cloud
    - Check terraform, for deploy specific Free-Tier for test at GCP. 


7. Build bdist(binary) and sdist (source) for the core-pipeline-beam project

Build
```
# 1st install wheel setuptools for packaging
pip install wheel setuptools

# 2nd build
python setup.py sdist bdist_wheel
```

Or execute the run bash
```
bash run_build_wheel.sh
```
    
8. Create a VM n1-micro

* Equivalent Cloud Shell
```
gcloud compute instances create vm-spot-beam-runner \
    --project=israndroid-data-core-project \
    --zone=us-central1-f \
    --machine-type=e2-micro \
    --network-interface=network-tier=PREMIUM,stack-type=IPV4_ONLY,subnet=default \
    --metadata=enable-osconfig=TRUE \
    --no-restart-on-failure \
    --maintenance-policy=TERMINATE \
    --provisioning-model=SPOT \
    --instance-termination-action=DELETE \
    --max-run-duration=3600s \
    --service-account=749795140382-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/trace.append \
    --create-disk=auto-delete=yes,boot=yes,device-name=vm-spot-beam-runner,image=projects/debian-cloud/global/images/debian-12-bookworm-v20250610,mode=rw,size=10,type=pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --labels=goog-ops-agent-policy=v2-x86-template-1-4-0,goog-ec-src=vm_add-gcloud \
    --reservation-affinity=none \
&& \
printf 'agentsRule:\n  packageState: installed\n  version: latest\ninstanceFilter:\n  inclusionLabels:\n  - labels:\n      goog-ops-agent-policy: v2-x86-template-1-4-0\n' > config.yaml \
&& \
gcloud compute instances ops-agents policies create goog-ops-agent-v2-x86-template-1-4-0-us-central1-f \
    --project=israndroid-data-core-project \
    --zone=us-central1-f \
    --file=config.yaml \
&& \
gcloud compute resource-policies create snapshot-schedule default-schedule-1 \
    --project=israndroid-data-core-project \
    --region=us-central1 \
    --max-retention-days=14 \
    --on-source-disk-delete=keep-auto-snapshots \
    --daily-schedule \
    --start-time=17:00 \
&& \
gcloud compute disks add-resource-policies vm-spot-beam-runner \
    --project=israndroid-data-core-project \
    --zone=us-central1-f \
    --resource-policies=projects/israndroid-data-core-project/regions/us-central1/resourcePolicies/default-schedule-1
```

* Equivalent Terraform
```
# This code is compatible with Terraform 4.25.0 and versions that are backwards compatible to 4.25.0.
# For information about validating this Terraform code, see https://developer.hashicorp.com/terraform/tutorials/gcp-get-started/google-cloud-platform-build#format-and-validate-the-configuration

resource "google_compute_instance" "vm-spot-beam-runner" {
  boot_disk {
    auto_delete = true
    device_name = "vm-spot-beam-runner"

    initialize_params {
      image = "projects/debian-cloud/global/images/debian-12-bookworm-v20250610"
      size  = 10
      type  = "pd-balanced"
    }

    mode = "READ_WRITE"
  }

  can_ip_forward      = false
  deletion_protection = false
  enable_display      = false

  labels = {
    goog-ec-src           = "vm_add-tf"
    goog-ops-agent-policy = "v2-x86-template-1-4-0"
  }

  machine_type = "e2-micro"

  metadata = {
    enable-osconfig = "TRUE"
  }

  name = "vm-spot-beam-runner"

  network_interface {
    access_config {
      network_tier = "PREMIUM"
    }

    queue_count = 0
    stack_type  = "IPV4_ONLY"
    subnetwork  = "projects/israndroid-data-core-project/regions/us-central1/subnetworks/default"
  }

  scheduling {
    automatic_restart   = false
    on_host_maintenance = "TERMINATE"
    preemptible         = false
    provisioning_model  = "SPOT"
  }

  service_account {
    email  = "749795140382-compute@developer.gserviceaccount.com"
    scopes = ["https://www.googleapis.com/auth/devstorage.read_only", "https://www.googleapis.com/auth/logging.write", "https://www.googleapis.com/auth/monitoring.write", "https://www.googleapis.com/auth/service.management.readonly", "https://www.googleapis.com/auth/servicecontrol", "https://www.googleapis.com/auth/trace.append"]
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = false
    enable_vtpm                 = true
  }

  zone = "us-central1-f"
}

module "ops_agent_policy" {
  source          = "github.com/terraform-google-modules/terraform-google-cloud-operations/modules/ops-agent-policy"
  project         = "israndroid-data-core-project"
  zone            = "us-central1-f"
  assignment_id   = "goog-ops-agent-v2-x86-template-1-4-0-us-central1-f"
  agents_rule = {
    package_state = "installed"
    version = "latest"
  }
  instance_filter = {
    all = false
    inclusion_labels = [{
      labels = {
        goog-ops-agent-policy = "v2-x86-template-1-4-0"
      }
    }]
  }
}
```

* BigQuery bq commands

At VM
```
mkdir schemas
```

```
bq show --schema --format=prettyjson israndroid-data-core-project:temp_data_core.house_price_rm > /schemas/house_price_rm.json
```

Upload to bucket data-core project
```
gsutil cp house_price_rm.json "gs://data-core-project-landing-zone/data_lake_core_web_scrapper/raw_schemas/house_price_rm.json"
```

# Develop TODOs

- [ ] define common Main for run different pipelines using commons Ptransforms and utils