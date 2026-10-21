import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebar: SidebarsConfig = {
  apisidebar: [
    {
      type: "doc",
      id: "api-reference/rest/exception-management-api",
    },
    {
      type: "category",
      label: "Exceptions",
      link: {
        type: "doc",
        id: "api-reference/rest/exceptions",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/list-exceptions",
          label: "List exception cases",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/reject-create-exception",
          label: "Cases are never created over REST (always 405)",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/get-exception",
          label: "Get an exception case",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/triage-exception",
          label: "Triage a case",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/assign-exception",
          label: "Assign a case",
          className: "api-method post",
        },
        {
          type: "doc",
          id: "api-reference/rest/resolve-exception",
          label: "Resolve a case",
          className: "api-method post",
        },
      ],
    },
    {
      type: "category",
      label: "Exception policies",
      link: {
        type: "doc",
        id: "api-reference/rest/exception-policies",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/list-exception-policies",
          label: "List the handling policies",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/declare-exception-policy",
          label: "Declare a new version of a handling policy",
          className: "api-method put",
        },
      ],
    },
    {
      type: "category",
      label: "Health",
      link: {
        type: "doc",
        id: "api-reference/rest/health",
      },
      items: [
        {
          type: "doc",
          id: "api-reference/rest/healthz",
          label: "Liveness probe",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/readyz",
          label: "Readiness probe",
          className: "api-method get",
        },
        {
          type: "doc",
          id: "api-reference/rest/metrics",
          label: "Prometheus metrics",
          className: "api-method get",
        },
      ],
    },
  ],
};

export default sidebar.apisidebar;
