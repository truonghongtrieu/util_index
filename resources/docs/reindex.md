Reindex
====

    POST /reindex?jwt=ADMIN_ADMIN_JWT


- 204 If ok
- 403 Permission denied

## Reindex a portal

    POST /reindex/PORTAL_NAME?jwt=ADMIN_ADMIN_JWT

- 204 OK
- 403 Permission denied

## Reindex certain parts

    POST /task -d {
        "title":         STRING,    # Descriptive task
        "instance":      STRING,    # Certain portal to be reindexed.
        "handlers":
            - STRING[],  # Certain part of portal data to be reindexed.
            - if handler start with #, #index will forward the message to microservice to execute their logic:
                - POST #service/re-index?jwt=ROOT_JWT                    — If instance is not given.
                - POST #service/re-index?jwt=ROOT_JWT&portalId=PORTAL_ID - If instance is given.
        "max_num_items": INT,       # …?
        "execute":       0 | 1,     # …?
        "index":         STRING,    # …?
        "alias":         0 | 1      # …?
    }

### Available handlers

Can be fetched by `GET /reindex/handlers?jwt=ROOT_JWT`

- `user`
- `account`
- `portal`
- `portal-config`
- `lo`
- `lo_share`
- `lo_content_sharing`
- `enrolment`
- `enrolment_share`
- `enrolment_revision`
- `account_enrolment`
- `asm_submission`
- `asm_submission_revision`
- `group`
- `mail`
- `eck-data`
- `payment_transaction`
- `quiz_user_answer`
- `eck`
- `manual_record`
- `coupon`
- `credit`
- `event`
- `award`
- `award_item`
- `award_item_manual`
- `award_enrolment`
- `award_enrolment_revision`
- `award_manual_enrolment`
- `plan`
- `contract`
- `metric`
