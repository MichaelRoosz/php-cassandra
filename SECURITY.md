# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub: on the repository's **Security** tab, choose **Report a vulnerability**. Do not open a public issue.

Include what you found, how to reproduce it, and which versions are affected.

## Supported versions

| Version | Security fixes |
|---|---|
| 2.x | Yes |
| 1.x | No |

## Hardening your deployment

- Use TLS with peer verification on any network you do not fully trust. Protocol v4, the newest ScyllaDB offers, has no checksums, so without TLS a corrupted response can reach your application as wrong data.
- Exceptions and logs can contain query context. Passwords and TLS key passphrases are kept out of them, but treat them as internal data.
