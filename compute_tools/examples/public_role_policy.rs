//! Emits the production provisioning SQL for the native PostgreSQL acceptance fixture.
use compute_api::spec::LakeonPublicLevel;
fn main() {
    for (name, level) in [
        ("native_owner", LakeonPublicLevel::Admin),
        ("native_reader", LakeonPublicLevel::Reader),
        ("native_writer", LakeonPublicLevel::Writer),
    ] {
        println!(
            "{}",
            compute_tools::public_roles::provision(
                name,
                "LOGIN PASSWORD 'local-public-role-123'",
                name == "native_owner" || std::env::var_os("PUBLIC_ROLE_EXISTING").is_some()
            )
        );
        println!(
            ";{}",
            compute_tools::public_roles::grants("postgres", "native_owner", name, &level)
        );
    }
}
