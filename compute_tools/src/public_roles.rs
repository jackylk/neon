//! Least-privilege provisioning for Lakeon customer roles.
use crate::pg_helpers::Escaping;

pub fn provision(name: &str, options: &str, exists: bool) -> String {
    let verb = if exists { "ALTER" } else { "CREATE" };
    let name_literal = crate::pg_helpers::escape_literal(name);
    let body = format!(
        "DECLARE parent record; BEGIN FOR parent IN SELECT r.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r ON r.oid=m.roleid JOIN pg_catalog.pg_roles child ON child.oid=m.member WHERE child.rolname={name_literal} LOOP EXECUTE pg_catalog.format('REVOKE %I FROM %I',parent.rolname,{name_literal}); END LOOP; END"
    );
    format!(
        "{verb} ROLE {name} {options} NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS INHERIT; DO {body}",
        name = name.to_string().pg_quote(),
        body = crate::pg_helpers::escape_literal(&body)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_role_never_gets_platform_membership_or_escalation() {
        for exists in [false, true] {
            let sql = provision("customer", "LOGIN PASSWORD NULL", exists);
            assert!(sql.contains("NOCREATEROLE"));
            assert!(sql.contains("NOSUPERUSER"));
            assert!(!sql.contains("IN ROLE"));
            assert!(sql.contains("REVOKE"));
        }
    }
    #[test]
    fn disabled_public_identity_does_not_gain_login_on_restart() {
        use crate::pg_helpers::RoleExt;
        let role = compute_api::spec::Role {
            name: "disabled".into(),
            encrypted_password: None,
            options: None,
            lakeon_public_access: Some(compute_api::spec::LakeonPublicAccess {
                level: compute_api::spec::LakeonPublicLevel::Reader,
                enabled: false,
            }),
        };
        assert!(role.to_pg_options().contains("NOLOGIN"));
    }
}

pub fn grants(
    database: &str,
    owner: &str,
    role: &str,
    level: &compute_api::spec::LakeonPublicLevel,
) -> String {
    use compute_api::spec::LakeonPublicLevel;
    let db = database.to_string().pg_quote();
    let owner = owner.to_string().pg_quote();
    let role = role.to_string().pg_quote();
    let (tables, sequences, schema) = match level {
        LakeonPublicLevel::Reader => ("SELECT", None, "USAGE"),
        LakeonPublicLevel::Writer => (
            "SELECT, INSERT, UPDATE, DELETE",
            Some("USAGE, SELECT"),
            "USAGE",
        ),
        LakeonPublicLevel::Admin => ("ALL PRIVILEGES", Some("ALL PRIVILEGES"), "ALL PRIVILEGES"),
    };
    let mut sql = format!(
        "REVOKE CREATE ON SCHEMA public FROM PUBLIC; REVOKE TEMPORARY ON DATABASE {db} FROM PUBLIC; GRANT CONNECT ON DATABASE {db} TO {role}; REVOKE ALL PRIVILEGES ON SCHEMA public FROM {role}; REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM {role}; REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM {role}; ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA public REVOKE ALL PRIVILEGES ON TABLES FROM {role}; ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA public REVOKE ALL PRIVILEGES ON SEQUENCES FROM {role}; GRANT {schema} ON SCHEMA public TO {role}; GRANT {tables} ON ALL TABLES IN SCHEMA public TO {role}; ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA public GRANT {tables} ON TABLES TO {role};"
    );
    if let Some(sequences) = sequences {
        sql.push_str(&format!("GRANT {sequences} ON ALL SEQUENCES IN SCHEMA public TO {role}; ALTER DEFAULT PRIVILEGES FOR ROLE {owner} IN SCHEMA public GRANT {sequences} ON SEQUENCES TO {role};"));
    }
    if matches!(level, LakeonPublicLevel::Admin) {
        sql.push_str(&format!("GRANT ALL PRIVILEGES ON DATABASE {db} TO {role};"));
    }
    sql
}
