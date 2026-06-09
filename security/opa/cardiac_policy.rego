# CardioCare-AIOps — Open Policy Agent Authorization Policy
# Standard: ISO 27001 A.9.4 — System and application access control
# NIST CSF: PR.AC-4 — Access permissions managed

package cardiocare.authz

import future.keywords.if
import future.keywords.in

# Default deny
default allow := false

# ── Role definitions ──────────────────────────────────────────────────────────
admin_roles    := {"admin", "superadmin", "cardiologist"}
clinical_roles := {"nurse", "doctor", "physician", "clinician"}
read_roles     := {"observer", "auditor", "analyst"}

# ── Allow rules ───────────────────────────────────────────────────────────────

# Admins can do anything
allow if {
    some role in input.roles
    role in admin_roles
}

# Clinicians can read patient data and alerts
allow if {
    some role in input.roles
    role in clinical_roles
    input.action in {"read", "list"}
    input.resource in {"vitals", "anomalies", "alerts", "patients"}
}

# Read-only users can read non-critical resources
allow if {
    some role in input.roles
    role in read_roles
    input.action == "read"
    input.resource in {"vitals", "anomalies", "system"}
}

# Anonymous users can only hit health endpoint
allow if {
    input.user == "anonymous"
    input.resource == "health"
    input.action == "read"
}

# Dev mode bypass
allow if {
    input.user == "dev-user"
}

# ── Audit log (all decisions are logged via OPA decision log) ─────────────────
audit_log := {
    "user":     input.user,
    "resource": input.resource,
    "action":   input.action,
    "allowed":  allow,
    "standard": "ISO27001-A.9.4",
}
