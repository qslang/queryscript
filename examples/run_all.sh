#!/bin/bash

set -euxo pipefail

composite parse examples/dau/schema.co
composite parse examples/dau/local.co
composite parse examples/crm/schema.co
composite parse examples/crm/local.co
