use anyhow::{Result, anyhow};
use axum::{RequestExt, body::Body};
use axum_extra::{
    TypedHeader,
    headers::{Authorization, authorization::Bearer},
};
use compute_api::requests::{COMPUTE_AUDIENCE, ComputeClaims, ComputeClaimsScope};
use futures::future::BoxFuture;
use http::{Request, Response, StatusCode};
use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation, jwk::JwkSet};
use tower_http::auth::AsyncAuthorizeRequest;
use tracing::{debug, warn};

use crate::http::JsonResponse;

#[derive(Clone, Debug)]
pub(in crate::http) struct Authorize {
    compute_id: String,
    // BEGIN HADRON
    // Hadron instance ID. Only set if it's a Lakebase V1 a.k.a. Hadron instance.
    instance_id: Option<String>,
    // END HADRON
    jwks: JwkSet,
    validation: Validation,
}

impl Authorize {
    pub fn new(compute_id: String, instance_id: Option<String>, jwks: JwkSet) -> Self {
        let mut validation = Validation::new(Algorithm::EdDSA);

        validation.validate_exp = true;
        // Unused by the control plane
        validation.validate_nbf = false;
        // Unused by the control plane
        validation.validate_aud = false;
        validation.set_audience(&[COMPUTE_AUDIENCE]);
        // Nothing is currently required
        validation.set_required_spec_claims(&[] as &[&str; 0]);

        Self {
            compute_id,
            instance_id,
            jwks,
            validation,
        }
    }
}

impl AsyncAuthorizeRequest<Body> for Authorize {
    type RequestBody = Body;
    type ResponseBody = Body;
    type Future = BoxFuture<'static, Result<Request<Body>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, mut request: Request<Body>) -> Self::Future {
        let compute_id = self.compute_id.clone();
        let is_hadron_instance = self.instance_id.is_some();
        let jwks = self.jwks.clone();
        let validation = self.validation.clone();

        Box::pin(async move {
            // BEGIN HADRON
            // In Hadron deployments the "external" HTTP endpoint on compute_ctl can only be
            // accessed by trusted components (enforced by dblet network policy), so we can bypass
            // all auth here.
            if is_hadron_instance {
                return Ok(request);
            }
            // END HADRON

            let TypedHeader(Authorization(bearer)) = request
                .extract_parts::<TypedHeader<Authorization<Bearer>>>()
                .await
                .map_err(|_| {
                    JsonResponse::error(StatusCode::BAD_REQUEST, "invalid authorization token")
                })?;

            let data = match Self::verify(&jwks, bearer.token(), &validation) {
                Ok(claims) => claims,
                Err(e) => return Err(JsonResponse::error(StatusCode::UNAUTHORIZED, e)),
            };

            match data.claims.scope {
                // TODO: We should validate audience for every token, but
                // instead of this ad-hoc validation, we should turn
                // [`Validation::validate_aud`] on. This is merely a stopgap
                // while we roll out `aud` deployment. We return a 401
                // Unauthorized because when we eventually do use
                // [`Validation`], we will hit the above `Err` match arm which
                // returns 401 Unauthorized.
                Some(ComputeClaimsScope::Admin) => {
                    let Some(ref audience) = data.claims.audience else {
                        return Err(JsonResponse::error(
                            StatusCode::UNAUTHORIZED,
                            "missing audience in authorization token claims",
                        ));
                    };

                    if !audience.iter().any(|a| a == COMPUTE_AUDIENCE) {
                        return Err(JsonResponse::error(
                            StatusCode::UNAUTHORIZED,
                            "invalid audience in authorization token claims",
                        ));
                    }
                }

                // If the scope is not [`ComputeClaimsScope::Admin`], then we
                // must validate the compute_id
                _ => {
                    let Some(ref claimed_compute_id) = data.claims.compute_id else {
                        return Err(JsonResponse::error(
                            StatusCode::FORBIDDEN,
                            "missing compute_id in authorization token claims",
                        ));
                    };

                    if *claimed_compute_id != compute_id {
                        return Err(JsonResponse::error(
                            StatusCode::FORBIDDEN,
                            "invalid compute ID in authorization token claims",
                        ));
                    }
                }
            }

            // Make claims available to any subsequent middleware or request
            // handlers
            request.extensions_mut().insert(data.claims);

            Ok(request)
        })
    }
}

impl Authorize {
    /// Verify the token using the JSON Web Key set and return the token data.
    fn verify(
        jwks: &JwkSet,
        token: &str,
        validation: &Validation,
    ) -> Result<TokenData<ComputeClaims>> {
        debug_assert!(!jwks.keys.is_empty());

        debug!("verifying compute authorization token");
        let header=jsonwebtoken::decode_header(token)?;

        for jwk in jwks.keys.iter() {
            // Pin verification to each key's declared algorithm. Never infer an
            // RSA algorithm from the token or accept PS256/RS256 interchangeably.
            let algorithm=match jwk.common.key_algorithm {
                Some(jsonwebtoken::jwk::KeyAlgorithm::PS256)=>Algorithm::PS256,
                Some(jsonwebtoken::jwk::KeyAlgorithm::RS256)=>Algorithm::RS256,
                Some(jsonwebtoken::jwk::KeyAlgorithm::EdDSA)|None=>Algorithm::EdDSA,
                _=>continue,
            };
            if header.alg!=algorithm || header.kid.as_ref().is_some_and(|kid|jwk.common.key_id.as_ref()!=Some(kid)){continue;}
            let mut key_validation=validation.clone();key_validation.algorithms=vec![algorithm];
            let decoding_key = match DecodingKey::from_jwk(jwk) {
                Ok(key) => key,
                Err(e) => {
                    warn!(
                        "failed to construct decoding key from {}: {}",
                        jwk.common.key_id.as_deref().unwrap_or("unlabelled"),
                        e
                    );

                    continue;
                }
            };

            match jsonwebtoken::decode::<ComputeClaims>(token, &decoding_key, &key_validation) {
                Ok(data) => return Ok(data),
                Err(e) => {
                    warn!(
                        "failed to decode authorization token using {}: {}",
                        jwk.common.key_id.as_deref().unwrap_or("unlabelled"),
                        e
                    );

                    continue;
                }
            }
        }

        Err(anyhow!("failed to verify authorization token"))
    }
}


#[cfg(test)]
mod lakeon_security_tests {
    use super::*;
    use base64::Engine;
    #[tokio::test]
    async fn ps256_management_token_is_verified_and_cannot_change_compute_or_algorithm() {
        let directory=tempfile::tempdir().unwrap();let path=directory.path().join("test-only.pem");
        assert!(std::process::Command::new("openssl").args(["genpkey","-algorithm","RSA","-pkeyopt","rsa_keygen_bits:2048","-out"]).arg(&path).output().unwrap().status.success());
        let modulus=std::process::Command::new("openssl").args(["rsa","-noout","-modulus","-in"]).arg(&path).output().unwrap();assert!(modulus.status.success());
        let line=String::from_utf8(modulus.stdout).unwrap();let hex=line.trim().strip_prefix("Modulus=").unwrap();
        let bytes=(0..hex.len()).step_by(2).map(|i|u8::from_str_radix(&hex[i..i+2],16).unwrap()).collect::<Vec<_>>();
        let n=base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        let jwks:JwkSet=serde_json::from_value(serde_json::json!({"keys":[{"kty":"RSA","alg":"PS256","kid":"test-only","n":n,"e":"AQAB"}]})).unwrap();
        let private=std::fs::read(path).unwrap();let key=jsonwebtoken::EncodingKey::from_rsa_pem(&private).unwrap();
        let expiry=std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs()+60;
        let claims=serde_json::json!({"compute_id":"compute-one","exp":expiry});
        let token=jsonwebtoken::encode(&jsonwebtoken::Header::new(Algorithm::PS256),&claims,&key).unwrap();
        let mut authorize=Authorize::new("compute-one".to_owned(),None,jwks.clone());
        let request=||Request::builder().header("Authorization",format!("Bearer {token}")).body(Body::empty()).unwrap();
        assert!(authorize.authorize(request()).await.is_ok(),"Valid platform PS256 token must be accepted");
        let mut wrong_compute=Authorize::new("compute-two".to_owned(),None,jwks.clone());
        assert!(wrong_compute.authorize(request()).await.is_err());
        let wrong_algorithm=jsonwebtoken::encode(&jsonwebtoken::Header::new(Algorithm::RS256),&claims,&key).unwrap();
        assert!(authorize.authorize(Request::builder().header("Authorization",format!("Bearer {wrong_algorithm}")).body(Body::empty()).unwrap()).await.is_err(),"A PS256 JWK must not accept an RS256 token");
    }
}
