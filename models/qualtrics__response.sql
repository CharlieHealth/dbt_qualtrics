WITH
    response AS (
        SELECT *
        FROM {{ ref('int_qualtrics__responses') }}
    ),

    question_option AS (

        SELECT *
        FROM {{ var('question_option') }}
    ),

    question AS (

        SELECT *
        FROM {{ ref('int_qualtrics__question') }}
    ),

    survey AS (

        SELECT *
        FROM {{ var('survey') }}
    ),

    embedded_data AS (

        SELECT *
        FROM {{ ref('int_qualtrics__survey_embedded_data') }}
    ),

    contacts AS (

        SELECT *
        FROM {{ ref('int_qualtrics__contacts') }}
    ),

    rollup_contacts AS (

        SELECT
            email,
            source_relation,
            MAX(email_domain) AS email_domain,
            MAX(first_name) AS first_name,
            MAX(last_name) AS last_name,
            MAX(language) AS language,
            MAX(external_data_reference) AS external_data_reference,
            {{ fivetran_utils.max_bool(boolean_field="is_xm_directory_contact") }} AS is_xm_directory_contact,
        {{ fivetran_utils.max_bool(boolean_field="is_research_core_contact") }} AS is_research_core_contact

        FROM contacts
        GROUP BY 1, 2
    ),

    parsed_response AS (
        SELECT  --multiple-choice questions may have comma-separate values as choices in the value field.  process all MC questions as if they hold multiple values
                --the LATERAL functionality cannot be on the left side of a join, so do it here before joining on the question_options for the final assembly
                --there is handling needed due to how Qualtrics returns the data to us.  for a finished survey, they return a record for every question (answered or not)
                --  and for every MC question, we recieve a record for every text-input choice (selected or not)
                --  we only want to keep the text-input choices if they were explicitly selected and we'll toss the record if not
            response.*,
            COALESCE(response.question_option_key, try_to_decimal(split_value.value, 10, 0)) AS parsed_question_option_key,
            split_value.value AS parsed_value,
            question.question_description,
            question.question_type,
            question.block_id,
            question.block_description,
            question.block_question_randomization,
            question.block_type,
            question.block_visibility,
            question.validation_setting_force_response,
            question.validation_setting_force_response_type,
            question.validation_setting_type,
            question.is_question_deleted,
            question.is_block_deleted
        FROM response
        LEFT JOIN question
            ON
                response.question_id = question.question_id
                AND response.survey_id = question.survey_id
                AND COALESCE(response.sub_question_key, -1) = COALESCE(question.sub_question_key, -1),  --not all questions have sub_questions
            LATERAL split_to_table(response.value, ',') AS split_value
        WHERE
            question.question_type = 'MC'
            AND response.sub_question_text IS NULL --split the response.value ONLY if it is a non-text style of input
            AND (
                NOT EXISTS ( --toss the record if there is a separate explicit choice made for a text input response, ie if option 5 is text input and populated, keep the text value, rather than the value of 5
                    SELECT 1
                    FROM response AS text_response
                    WHERE
                        text_response.sub_question_text IS NOT NULL
                        AND NULLIF(TRIM(text_response.value), '') IS NOT NULL
                        AND text_response.question_option_key = TRY_TO_DECIMAL(split_value.value, 10, 0)
                        AND text_response.survey_response_id = response.survey_response_id
                        AND text_response.question_id = response.question_id
                )
            )
        UNION
        SELECT  --some special handling is still needed
            response.*,
            COALESCE(response.question_option_key, TRY_TO_DECIMAL(response.value)) AS parsed_question_option_key,
            response.value AS parsed_value,
            question.question_description,
            question.question_type,
            question.block_id,
            question.block_description,
            question.block_question_randomization,
            question.block_type,
            question.block_visibility,
            question.validation_setting_force_response,
            question.validation_setting_force_response_type,
            question.validation_setting_type,
            question.is_question_deleted,
            question.is_block_deleted
        FROM response
        LEFT JOIN question
            ON
                response.question_id = question.question_id
                AND response.survey_id = question.survey_id
                AND COALESCE(response.sub_question_key, -1) = COALESCE(question.sub_question_key, -1)
        WHERE
            question.question_type != 'MC' --no special handling needed for non-MC questions
            OR (
                question.question_type = 'MC' --if someone responds with text input for a MC question, keep that record
                AND response.sub_question_text IS NOT NULL
                AND nULLIF(TRIM(Response.value), '') IS NOT NULL
            )
    ),

    final AS (

        SELECT
            parsed_response.question_response_id,
            parsed_response.parsed_question_option_key,
            parsed_response.parsed_value,
            parsed_response.survey_response_id,
            parsed_response.survey_id,
            parsed_response.question_id,
            parsed_response.question_text,
            parsed_response.sub_question_key,
            parsed_response.sub_question_text,
            parsed_response.question_option_key,
            parsed_response.value,
            parsed_response.loop_id,
            parsed_response.distribution_channel,
            parsed_response.survey_response_status,
            parsed_response.survey_progress,
            parsed_response.duration_in_seconds,
            parsed_response.is_finished_with_survey,
            parsed_response.survey_finished_at,
            parsed_response.survey_response_last_modified_at,
            parsed_response.survey_response_recorded_at,
            parsed_response.survey_response_started_at,
            parsed_response.recipient_email,
            parsed_response.recipient_first_name,
            parsed_response.recipient_last_name,
            parsed_response.user_language,
            parsed_response.ip_address,
            parsed_response.location_latitude,
            parsed_response.location_longitude,
            parsed_response.source_relation,

            question_option.recode_value,
            question_option.text AS question_option_text,
            rollup_contacts.first_name,
            rollup_contacts.last_name,
            rollup_contacts.email_domain,
            rollup_contacts.language AS contact_language,
            rollup_contacts.external_data_reference AS contact_external_data_reference,
            rollup_contacts.is_xm_directory_contact,
            rollup_contacts.is_research_core_contact,

            embedded_data.embedded_data,
            survey.survey_name,
            survey.survey_status,
            survey.project_category,
            survey.project_type,
            survey.brand_base_url,

            -- most question fields are included, as there is no question end model. join with `int_qualtrics__question` and `parsed_response` to bring in more
            parsed_response.question_description,
            parsed_response.question_type,
            parsed_response.block_id,
            parsed_response.block_description,
            parsed_response.block_question_randomization,
            parsed_response.block_type,
            parsed_response.block_visibility,
            parsed_response.validation_setting_force_response,
            parsed_response.validation_setting_force_response_type,
            parsed_response.validation_setting_type,
            parsed_response.is_question_deleted,
            parsed_response.is_block_deleted

        FROM parsed_response

        LEFT JOIN question_option
            ON
                parsed_response.question_id = question_option.question_id
                AND parsed_response.survey_id = question_option.survey_id
                AND parsed_response.parsed_question_option_key::varchar = question_option.recode_value::varchar --remove these CASTS once the CH MBC survey is corrected and changes a couple of recode values of '.'
                AND parsed_response.source_relation = question_option.source_relation
                AND question_option.is_deleted = FALSE

        LEFT JOIN survey
            ON
                parsed_response.survey_id = survey.survey_id
                AND parsed_response.source_relation = survey.source_relation

        LEFT JOIN embedded_data
            ON
                parsed_response.survey_response_id = embedded_data.response_id
                AND parsed_response.source_relation = embedded_data.source_relation

        LEFT JOIN rollup_contacts
            ON
                parsed_response.recipient_email = rollup_contacts.email
                AND parsed_response.source_relation = rollup_contacts.source_relation

    )

SELECT *
FROM final
