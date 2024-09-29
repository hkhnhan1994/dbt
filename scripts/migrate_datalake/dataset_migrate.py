datalake_config = {
    "H1_HAMP": [
        {
            "table_name": "Orders",
            "primary_key": "OrderId",
            "update_timestamp": "Timestamp",
        },
        {
            "table_name": "OrdersStatuses",
            "primary_key": "OrderStatusId",
            "update_timestamp": "Timestamp",
        },
    ],
    "H1_HKVK": [
        {
            "table_name": "RequestCache",
            "primary_key": ["RequestCacheID", "CacheKey"],
            "type": "base_template_pre_hash",
            "columns_pre_hash": ["responsedata"],
            "update_timestamp": "ResponseTimeUTC",
        },
    ],
    "H1_HKLC": [
        {
            "table_name": "aselect_loginhistory",
            "primary_key": "loginid",
            "update_timestamp": "logintime",
        },
        {
            "table_name": "DA_relation",
            "primary_key": "relationid",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "DA_relation_audit",
            "primary_key": "relation_audit_id",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "DA_relationrole",
            "primary_key": ["relationtype", "relationrole"],
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "tblAbonnement",
            "primary_key": "AbonnementID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblAbonnementType",
            "primary_key": "AbonnementTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblDienst",
            "primary_key": "DienstID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblDienstType",
            "primary_key": "DienstTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblFactType",
            "primary_key": "FactTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblGrootboek",
            "primary_key": "GrootBoekID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlant",
            "primary_key": "KlantID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnement",
            "primary_key": ["KlantID", "VolgNr", "AbonnementID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnementGebruik",
            "primary_key": ["KlantID", "VolgNr", "GebruikID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnementSysAttr",
            "primary_key": ["KlantID", "VolgNr", "SysteemID", "attribuutid"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantBankMachtiging",
            "primary_key": ["KlantID", "Code"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantDienst",
            "primary_key": ["KlantID", "VolgNr"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantSysAttr",
            "primary_key": ["KlantID", "SysteemID", "AttribuutID", "rowguid"],
        },
        {
            "table_name": "tblLand",
            "primary_key": "LandID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblOrganisatie",
            "primary_key": "OrganisatieID",
            "update_timestamp": "DateChange",
        },
        # { # table is unvailable
        #     "table_name": "tblOrganisatieBankMachtiging",
        #     "primary_key": "OrganisatieID",
        #     "update_timestamp": "DateChange",
        # },
        {
            "table_name": "tblSysteemAttribuut",
            "primary_key": ["SysteemID", "AttribuutID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblBTW",
            "primary_key": "BTWID",
            "update_timestamp": "DateChange",
        },
    ],
    "H1_HEHE": [  # not available on dev
        {
            "table_name": "AuditAuthenticationSet",
            "primary_key": "__auditid",
            "update_timestamp": "__auditdateutc",
        },
        {
            "table_name": "AuthenticationSet",
            "primary_key": "AuthenticationSetId",
        },
        {
            "table_name": "AuthorizationSubject",
            "primary_key": "AuthorizationSubjectId",
        },
        {
            "table_name": "ChainAuthorization",
            "primary_key": "ChainAuthorizationId",
        },
        {
            "table_name": "ChainAuthorizationService",
            "primary_key": "ChainAuthorizationServiceId",
            "update_timestamp": "StartDate",
        },
        {
            "table_name": "ChainedOrganisation",
            "primary_key": "ChainedOrganisationId",
            "update_timestamp": "StartDate",
        },
        {
            "table_name": "Group",
            "primary_key": "GroupId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "GroupUser",
            "primary_key": "GroupUserId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "IntermediateAuthorizationService",
            "primary_key": "ChainedAuthorizationServiceId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "Organisation",
            "primary_key": "OrganisationId",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "Person",
            "primary_key": "PersonId",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "PersonAttribute",
            "primary_key": ["PersonId", "SourceUrnId", "attributeurnid"],
            "update_timestamp": "VerifyDateUtc",
        },
        {
            "table_name": "PersonOrganisationLink",
            "primary_key": "PersonOrganisationLinkId",
            "update_timestamp": "LastServiceAuthorizationUse",
        },
        {
            "table_name": "PersonOrganisationRole",
            "primary_key": "PersonOrganisationRoleId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "PersonUsageTransactions",
            "primary_key": "PersonUsageTransactionId",
            "update_timestamp": "TransactionTime",
        },
        {
            "table_name": "ServiceAuthorization_1_13",
            "primary_key": "ServiceAuthorizationId",
            "update_timestamp": "FromDate",
        },
        {
            "table_name": "Urn",
            "primary_key": "UrnId",
        },
        {
            "table_name": "Urn",
            "primary_key": "UrnId",
        },
        {
            "table_name": "ISO_Country",
            "primary_key": "Alpha2",
        },
        {
            "table_name": "RegistrationLevel",
            "primary_key": "RegistrationLevelId",
        },
        {
            "table_name": "Roles",
            "primary_key": "Role",
        },
        # { # not exists
        #     "table_name": "rsin",
        #     "primary_key": ["CacheKey", "rsin"],
        #     "type": "manual_multi_pk",
        # },
        {
            "table_name": "Service",
            "primary_key": "ServiceId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "ServiceProvider",
            "primary_key": "ServiceProviderId",
            "update_timestamp": "InsertDate",
        },
    ],
    "H1_HEHR": [
        {
            "table_name": "admessageentry",
            "primary_key": ["report", "Origin"],
        },
        {
            "table_name": "mrcompanyentry",
            "primary_key": "report",
        },
        {
            "table_name": "mrmessageentry",
            "primary_key": ["report", "Origin"],
        },
        {
            "table_name": "mu2entry",
            "primary_key": ["jaar", "maand"],
        },
        {
            "table_name": "muentry",
            "primary_key": ["report", "level"],
        },
        {
            "table_name": "EherkenningRequests",
            "primary_key": "Id",
            "update_timestamp": "Timestamp",
        },
        {
            "table_name": "EherkenningResponses",
            "primary_key": "Id",
            "update_timestamp": "Timestamp",
        },
    ],
    "H1_HONB": [  # not available on dev
        {
            "table_name": "AuditOnboardingWorkflows",
            "primary_key": "AuditOnboardingWorkflowId",
            "update_timestamp": "Timestamp",
        },
        {
            "table_name": "BarcodeReferences",
            "primary_key": "BarcodeReferenceId",
        },
        {
            "table_name": "Companies",
            "primary_key": "CompanyId",
        },
        {
            "table_name": "OnboardingWorkflows",
            "primary_key": "OnboardingWorkflowId",
        },
        {
            "table_name": "UserOrganisations",
            "primary_key": ["UserId", "OrganisationId"],
        },
        {
            "table_name": "Users",
            "primary_key": "UserId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "UserTasks",
            "primary_key": "UserTaskId",
        },
    ],
    "H2_HKLC": [
        {
            "table_name": "aselect_loginhistory",
            "primary_key": "loginid",
            "update_timestamp": "logintime",
        },
        {
            "table_name": "DA_relation",
            "primary_key": "relationid",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "DA_relation_audit",
            "primary_key": "relation_audit_id",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "DA_relationrole",
            "primary_key": ["relationtype", "relationrole"],
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "tblAbonnement",
            "primary_key": "AbonnementID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblAbonnementType",
            "primary_key": "AbonnementTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblDienst",
            "primary_key": "DienstID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblDienstType",
            "primary_key": "DienstTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblFactType",
            "primary_key": "FactTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblGrootboek",
            "primary_key": "GrootBoekID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlant",
            "primary_key": "KlantID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnement",
            "primary_key": ["KlantID", "VolgNr", "AbonnementID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnementGebruik",
            "primary_key": ["KlantID", "VolgNr", "GebruikID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnementSysAttr",
            "primary_key": ["KlantID", "VolgNr", "SysteemID", "attribuutid"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantBankMachtiging",
            "primary_key": ["KlantID", "Code"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantDienst",
            "primary_key": ["KlantID", "VolgNr"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantSysAttr",
            "primary_key": ["KlantID", "SysteemID", "AttribuutID", "rowguid"],
        },
        {
            "table_name": "tblLand",
            "primary_key": "LandID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblOrganisatie",
            "primary_key": "OrganisatieID",
            "update_timestamp": "DateChange",
        },
        # {
        #     "table_name": "tblOrganisatieBankMachtiging",
        #     "primary_key": "OrganisatieID",
        #     "update_timestamp": "DateChange",
        # },
        {
            "table_name": "tblSysteemAttribuut",
            "primary_key": ["SysteemID", "AttribuutID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblBTW",
            "primary_key": "BTWID",
            "update_timestamp": "DateChange",
        },
    ],
    "H2_HEHE": [
        {
            "table_name": "AuditAuthenticationSet",
            "primary_key": "__auditid",
            "update_timestamp": "__auditdateutc",
        },
        {
            "table_name": "AuthenticationSet",
            "primary_key": "AuthenticationSetId",
        },
        {
            "table_name": "AuthorizationSubject",
            "primary_key": "AuthorizationSubjectId",
        },
        {
            "table_name": "ChainAuthorization",
            "primary_key": "ChainAuthorizationId",
        },
        {
            "table_name": "ChainAuthorizationService",
            "primary_key": "ChainAuthorizationServiceId",
            "update_timestamp": "StartDate",
        },
        {
            "table_name": "ChainedOrganisation",
            "primary_key": "ChainedOrganisationId",
            "update_timestamp": "StartDate",
        },
        {
            "table_name": "Group",
            "primary_key": "GroupId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "GroupUser",
            "primary_key": "GroupUserId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "IntermediateAuthorizationService",
            "primary_key": "ChainedAuthorizationServiceId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "Organisation",
            "primary_key": "OrganisationId",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "Person",
            "primary_key": "PersonId",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "PersonAttribute",
            "primary_key": ["PersonId", "SourceUrnId", "attributeurnid"],
            "update_timestamp": "VerifyDateUtc",
        },
        {
            "table_name": "PersonOrganisationLink",
            "primary_key": "PersonOrganisationLinkId",
            "update_timestamp": "LastServiceAuthorizationUse",
        },
        {
            "table_name": "PersonOrganisationRole",
            "primary_key": "PersonOrganisationRoleId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "PersonUsageTransactions",
            "primary_key": "PersonUsageTransactionId",
            "update_timestamp": "TransactionTime",
        },
        {
            "table_name": "ServiceAuthorization_1_13",
            "primary_key": "ServiceAuthorizationId",
            "update_timestamp": "FromDate",
        },
        {
            "table_name": "Urn",
            "primary_key": "UrnId",
        },
        {
            "table_name": "ISO_Country",
            "primary_key": "Alpha2",
        },
        {
            "table_name": "RegistrationLevel",
            "primary_key": "RegistrationLevelId",
        },
        {
            "table_name": "Roles",
            "primary_key": "Role",
        },
        # { # not exists
        #     "table_name": "rsin",
        #     "primary_key": ["CacheKey", "rsin"],
        #     "type": "manual_multi_pk",
        # },
        {
            "table_name": "Service",
            "primary_key": "ServiceId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "ServiceProvider",
            "primary_key": "ServiceProviderId",
            "update_timestamp": "InsertDate",
        },
    ],
    "H2_HEHR": [
        {
            "table_name": "admessageentry",
            "primary_key": ["report", "Origin"],
        },
        {
            "table_name": "mrcompanyentry",
            "primary_key": "report",
        },
        {
            "table_name": "mrmessageentry",
            "primary_key": ["report", "Origin"],
        },
        {
            "table_name": "mu2entry",
            "primary_key": ["jaar", "maand"],
        },
        {
            "table_name": "muentry",
            "primary_key": ["report", "level"],
        },
        {
            "table_name": "EherkenningRequests",
            "primary_key": "Id",
            "update_timestamp": "Timestamp",
        },
        {
            "table_name": "EherkenningResponses",
            "primary_key": "Id",
            "update_timestamp": "Timestamp",
        },
    ],
    "H2_HONB": [
        {
            "table_name": "AuditOnboardingWorkflows",
            "primary_key": "AuditOnboardingWorkflowId",
            "update_timestamp": "Timestamp",
        },
        {
            "table_name": "BarcodeReferences",
            "primary_key": "BarcodeReferenceId",
        },
        {
            "table_name": "Companies",
            "primary_key": "CompanyId",
        },
        {
            "table_name": "OnboardingWorkflows",
            "primary_key": "OnboardingWorkflowId",
        },
        {
            "table_name": "UserOrganisations",
            "primary_key": ["UserId", "OrganisationId"],
        },
        {
            "table_name": "Users",
            "primary_key": "UserId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "UserTasks",
            "primary_key": "UserTaskId",
        },
    ],
    "H3_HKLC": [
        {
            "table_name": "aselect_loginhistory",
            "primary_key": "loginid",
            "update_timestamp": "logintime",
        },
        {
            "table_name": "DA_relation",
            "primary_key": "relationid",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "DA_relation_audit",
            "primary_key": "relation_audit_id",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "DA_relationrole",
            "primary_key": ["relationtype", "relationrole"],
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "tblAbonnement",
            "primary_key": "AbonnementID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblAbonnementType",
            "primary_key": "AbonnementTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblDienst",
            "primary_key": "DienstID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblDienstType",
            "primary_key": "DienstTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblFactType",
            "primary_key": "FactTypeID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblGrootboek",
            "primary_key": "GrootBoekID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlant",
            "primary_key": "KlantID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnement",
            "primary_key": ["KlantID", "VolgNr", "AbonnementID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnementGebruik",
            "primary_key": ["KlantID", "VolgNr", "GebruikID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantAbonnementSysAttr",
            "primary_key": ["KlantID", "VolgNr", "SysteemID", "attribuutid"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantBankMachtiging",
            "primary_key": ["KlantID", "Code"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantDienst",
            "primary_key": ["KlantID", "VolgNr"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblKlantSysAttr",
            "primary_key": ["KlantID", "SysteemID", "AttribuutID", "rowguid"],
        },
        {
            "table_name": "tblLand",
            "primary_key": "LandID",
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblOrganisatie",
            "primary_key": "OrganisatieID",
            "update_timestamp": "DateChange",
        },
        # {
        #     "table_name": "tblOrganisatieBankMachtiging",
        #     "primary_key": "OrganisatieID",
        #     "update_timestamp": "DateChange",
        # },
        {
            "table_name": "tblSysteemAttribuut",
            "primary_key": ["SysteemID", "AttribuutID"],
            "update_timestamp": "DateChange",
        },
        {
            "table_name": "tblBTW",
            "primary_key": "BTWID",
            "update_timestamp": "DateChange",
        },
    ],
    "H3_HEHE": [
        {
            "table_name": "AuditAuthenticationSet",
            "primary_key": "__auditid",
            "update_timestamp": "__auditdateutc",
        },
        {
            "table_name": "AuthenticationSet",
            "primary_key": "AuthenticationSetId",
        },
        {
            "table_name": "AuthorizationSubject",
            "primary_key": "AuthorizationSubjectId",
        },
        {
            "table_name": "ChainAuthorization",
            "primary_key": "ChainAuthorizationId",
        },
        {
            "table_name": "ChainAuthorizationService",
            "primary_key": "ChainAuthorizationServiceId",
            "update_timestamp": "StartDate",
        },
        {
            "table_name": "ChainedOrganisation",
            "primary_key": "ChainedOrganisationId",
            "update_timestamp": "StartDate",
        },
        {
            "table_name": "Group",
            "primary_key": "GroupId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "GroupUser",
            "primary_key": "GroupUserId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "IntermediateAuthorizationService",
            "primary_key": "ChainedAuthorizationServiceId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "Organisation",
            "primary_key": "OrganisationId",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "Person",
            "primary_key": "PersonId",
            "update_timestamp": "LastUpdatedDate",
        },
        {
            "table_name": "PersonAttribute",
            "primary_key": ["PersonId", "SourceUrnId", "attributeurnid"],
            "update_timestamp": "VerifyDateUtc",
        },
        {
            "table_name": "PersonOrganisationLink",
            "primary_key": "PersonOrganisationLinkId",
            "update_timestamp": "LastServiceAuthorizationUse",
        },
        {
            "table_name": "PersonOrganisationRole",
            "primary_key": "PersonOrganisationRoleId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "PersonUsageTransactions",
            "primary_key": "PersonUsageTransactionId",
            "update_timestamp": "TransactionTime",
        },
        {
            "table_name": "ServiceAuthorization_1_13",
            "primary_key": "ServiceAuthorizationId",
            "update_timestamp": "FromDate",
        },
        {
            "table_name": "Urn",
            "primary_key": "UrnId",
        },
        {
            "table_name": "ISO_Country",
            "primary_key": "Alpha2",
        },
        {
            "table_name": "RegistrationLevel",
            "primary_key": "RegistrationLevelId",
        },
        {
            "table_name": "Roles",
            "primary_key": "Role",
        },
        # { # not exists
        #     "table_name": "rsin",
        #     "primary_key": ["CacheKey", "rsin"],
        #     "type": "manual_multi_pk",
        # },
        {
            "table_name": "Service",
            "primary_key": "ServiceId",
            "update_timestamp": "InsertDate",
        },
        {
            "table_name": "ServiceProvider",
            "primary_key": "ServiceProviderId",
            "update_timestamp": "InsertDate",
        },
    ],
    "H3_HEHR": [
        {
            "table_name": "admessageentry",
            "primary_key": ["report", "Origin"],
        },
        {
            "table_name": "mrcompanyentry",
            "primary_key": "report",
        },
        {
            "table_name": "mrmessageentry",
            "primary_key": ["report", "Origin"],
        },
        {
            "table_name": "mu2entry",
            "primary_key": ["jaar", "maand"],
        },
        {
            "table_name": "muentry",
            "primary_key": ["report", "level"],
        },
        {
            "table_name": "EherkenningRequests",
            "primary_key": "Id",
            "update_timestamp": "Timestamp",
        },
        {
            "table_name": "EherkenningResponses",
            "primary_key": "Id",
            "update_timestamp": "Timestamp",
        },
    ],
    "H3_HONB": [
        {
            "table_name": "AuditOnboardingWorkflows",
            "primary_key": "AuditOnboardingWorkflowId",
            "update_timestamp": "Timestamp",
        },
        {
            "table_name": "BarcodeReferences",
            "primary_key": "BarcodeReferenceId",
        },
        {
            "table_name": "Companies",
            "primary_key": "CompanyId",
        },
        {
            "table_name": "OnboardingWorkflows",
            "primary_key": "OnboardingWorkflowId",
        },
        {
            "table_name": "UserOrganisations",
            "primary_key": ["UserId", "OrganisationId"],
        },
        {
            "table_name": "Users",
            "primary_key": "UserId",
            "update_timestamp": "CreationDate",
        },
        {
            "table_name": "UserTasks",
            "primary_key": "UserTaskId",
        },
    ],
}
