## Overview

**Custom Code** allows users to write Java code with data processing logic , upload Java jar packages
to [BladePipe](https://www.bladepipe.com/), and BladePipe automatically call these codes during **Full Data** and **Incremental** to achieve various data transformation processing purposes.

## Project Description

- **wide-table**
    - The fact table and dimension tables join processing code.
- **data-transform**
    - General data transformation code, e.g., doing operation changes, adding additional fields, filter data,and so on.
- **data-gather**
    - Aggregate sharding data code.e.g.,eliminate constraint conflicts, adding additional fields and so on.
- **data-compare**
    - Verification and correction with business logic.
- **business-alert**
    - Provides corresponding alarms based on stream data.

## Steps

- [Quick Start](https://doc.bladepipe.com/quick/quick_start)
- [Create a Custom Code DataJob](https://doc.bladepipe.com/operation/job_manage/create_job/create_process_job)
- DataJob running.

## References

- [Debug Custom Code](https://doc.bladepipe.com/operation/job_manage/job_op/debug_customer_code)
- [Log in Custom Code](https://doc.bladepipe.com/operation/job_manage/job_op/log_in_customer_code)
