#!/bin/bash

set -euxo pipefail

qvm parse examples/dau/schema.co
qvm parse examples/dau/local.co
qvm parse examples/crm/schema.co
qvm parse examples/crm/local.co
