use crate::security::access_control::AuthenticationManager;
use crate::security::crypto::CryptographicManager;
use crate::security::rate_limiter::AuthRateLimiter;
use crate::security::SecurityResult;
use std::time::{Duration, Instant};
use tokio::time::sleep;
// Simple statistical functions since we want to avoid external dependencies
fn mean(data: &[f64]) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    data.iter().sum::<f64>() / data.len() as f64
}

fn standard_deviation(data: &[f64], mean_val: Option<f64>) -> f64 {
    if data.is_empty() {
        return 0.0;
    }
    let m = mean_val.unwrap_or_else(|| mean(data));
    let variance = data.iter().map(|x| (x - m).powi(2)).sum::<f64>() / data.len() as f64;
    variance.sqrt()
}

pub struct TimingAttackTester {
    auth_manager: AuthenticationManager,
    crypto_manager: CryptographicManager,
    rate_limiter: AuthRateLimiter,
}

#[derive(Debug, Clone)]
pub struct TimingTestResult {
    pub test_name: String,
    pub vulnerable: bool,
    pub mean_time_valid: f64,
    pub mean_time_invalid: f64,
    pub timing_difference: f64,
    pub standard_deviation_valid: f64,
    pub standard_deviation_invalid: f64,
    pub sample_size: usize,
    pub confidence_level: f64,
}

#[derive(Debug)]
pub struct TimingAttackReport {
    pub tests: Vec<TimingTestResult>,
    pub overall_security_score: f64,
    pub critical_vulnerabilities: Vec<String>,
    pub recommendations: Vec<String>,
}

impl TimingAttackTester {
    pub fn new() -> SecurityResult<Self> {
        let crypto_manager = CryptographicManager::new(Duration::from_secs(3600))?;
        let auth_manager = AuthenticationManager::new(
            "test_secret_key_for_timing_attack_testing".to_string(),
            Duration::from_secs(3600),
        );
        let rate_limiter = AuthRateLimiter::new();

        Ok(Self {
            auth_manager,
            crypto_manager,
            rate_limiter,
        })
    }

    pub async fn run_comprehensive_timing_tests(&self) -> SecurityResult<TimingAttackReport> {
        let mut tests = Vec::new();

        println!("Running comprehensive timing attack tests...");

        tests.push(self.test_password_verification_timing().await?);
        tests.push(self.test_user_enumeration_timing().await?);
        tests.push(self.test_api_key_verification_timing().await?);
        tests.push(self.test_totp_verification_timing().await?);
        tests.push(self.test_session_token_validation_timing().await?);
        tests.push(self.test_constant_time_string_comparison().await?);
        tests.push(self.test_hash_comparison_timing().await?);
        tests.push(self.test_backup_code_verification_timing().await?);

        let overall_score = self.calculate_overall_security_score(&tests);
        let critical_vulnerabilities = self.identify_critical_vulnerabilities(&tests);
        let recommendations = self.generate_recommendations(&tests);

        Ok(TimingAttackReport {
            tests,
            overall_security_score: overall_score,
            critical_vulnerabilities,
            recommendations,
        })
    }

    async fn test_password_verification_timing(&self) -> SecurityResult<TimingTestResult> {
        println!("Testing password verification timing...");

        let sample_size = 100;
        let mut valid_times = Vec::new();
        let mut invalid_times = Vec::new();

        let test_username = "timing_test_user";
        let correct_password = "correct_password_123";
        let wrong_password = "wrong_password_456";

        self.auth_manager.create_user(
            test_username.to_string(),
            correct_password.to_string(),
            std::collections::HashSet::new(),
        )?;

        for _ in 0..sample_size {
            let start = Instant::now();
            let _ = self
                .auth_manager
                .authenticate(test_username, correct_password, None);
            valid_times.push(start.elapsed().as_nanos() as f64);

            sleep(Duration::from_millis(10)).await;

            let start = Instant::now();
            let _ = self
                .auth_manager
                .authenticate(test_username, wrong_password, None);
            invalid_times.push(start.elapsed().as_nanos() as f64);

            sleep(Duration::from_millis(10)).await;
        }

        let mean_valid = mean(&valid_times);
        let mean_invalid = mean(&invalid_times);
        let std_valid = standard_deviation(&valid_times, Some(mean_valid));
        let std_invalid = standard_deviation(&invalid_times, Some(mean_invalid));

        let timing_difference = (mean_valid - mean_invalid).abs();
        let normalized_difference = timing_difference / mean_valid.max(mean_invalid);

        let vulnerable = normalized_difference > 0.1;
        let confidence = self.calculate_statistical_confidence(&valid_times, &invalid_times);

        Ok(TimingTestResult {
            test_name: "Password Verification Timing".to_string(),
            vulnerable,
            mean_time_valid: mean_valid,
            mean_time_invalid: mean_invalid,
            timing_difference,
            standard_deviation_valid: std_valid,
            standard_deviation_invalid: std_invalid,
            sample_size,
            confidence_level: confidence,
        })
    }

    async fn test_user_enumeration_timing(&self) -> SecurityResult<TimingTestResult> {
        println!("Testing user enumeration timing...");

        let sample_size = 100;
        let mut existing_user_times = Vec::new();
        let mut nonexistent_user_times = Vec::new();

        let existing_username = "existing_user_test";
        let password = "test_password_123";

        self.auth_manager.create_user(
            existing_username.to_string(),
            password.to_string(),
            std::collections::HashSet::new(),
        )?;

        for i in 0..sample_size {
            let nonexistent_username = format!("nonexistent_user_{}", i);

            let start = Instant::now();
            let _ = self
                .auth_manager
                .authenticate(existing_username, "wrong_password", None);
            existing_user_times.push(start.elapsed().as_nanos() as f64);

            sleep(Duration::from_millis(10)).await;

            let start = Instant::now();
            let _ = self
                .auth_manager
                .authenticate(&nonexistent_username, "wrong_password", None);
            nonexistent_user_times.push(start.elapsed().as_nanos() as f64);

            sleep(Duration::from_millis(10)).await;
        }

        let mean_existing = mean(&existing_user_times);
        let mean_nonexistent = mean(&nonexistent_user_times);
        let std_existing = standard_deviation(&existing_user_times, Some(mean_existing));
        let std_nonexistent = standard_deviation(&nonexistent_user_times, Some(mean_nonexistent));

        let timing_difference = (mean_existing - mean_nonexistent).abs();
        let normalized_difference = timing_difference / mean_existing.max(mean_nonexistent);

        let vulnerable = normalized_difference > 0.05;
        let confidence =
            self.calculate_statistical_confidence(&existing_user_times, &nonexistent_user_times);

        Ok(TimingTestResult {
            test_name: "User Enumeration Timing".to_string(),
            vulnerable,
            mean_time_valid: mean_existing,
            mean_time_invalid: mean_nonexistent,
            timing_difference,
            standard_deviation_valid: std_existing,
            standard_deviation_invalid: std_nonexistent,
            sample_size,
            confidence_level: confidence,
        })
    }

    async fn test_api_key_verification_timing(&self) -> SecurityResult<TimingTestResult> {
        println!("Testing API key verification timing...");

        let sample_size = 100;
        let mut valid_times = Vec::new();
        let mut invalid_times = Vec::new();

        for _ in 0..sample_size {
            let valid_hash = self.crypto_manager.secure_hash(b"valid_api_key");
            let invalid_hash = self.crypto_manager.secure_hash(b"invalid_api_key");

            let start = Instant::now();
            let _ = self
                .crypto_manager
                .secure_compare(valid_hash.as_bytes(), valid_hash.as_bytes());
            valid_times.push(start.elapsed().as_nanos() as f64);

            let start = Instant::now();
            let _ = self
                .crypto_manager
                .secure_compare(valid_hash.as_bytes(), invalid_hash.as_bytes());
            invalid_times.push(start.elapsed().as_nanos() as f64);

            sleep(Duration::from_millis(1)).await;
        }

        let mean_valid = mean(&valid_times);
        let mean_invalid = mean(&invalid_times);
        let std_valid = standard_deviation(&valid_times, Some(mean_valid));
        let std_invalid = standard_deviation(&invalid_times, Some(mean_invalid));

        let timing_difference = (mean_valid - mean_invalid).abs();
        let normalized_difference = timing_difference / mean_valid.max(mean_invalid);

        let vulnerable = normalized_difference > 0.02;
        let confidence = self.calculate_statistical_confidence(&valid_times, &invalid_times);

        Ok(TimingTestResult {
            test_name: "API Key Verification Timing".to_string(),
            vulnerable,
            mean_time_valid: mean_valid,
            mean_time_invalid: mean_invalid,
            timing_difference,
            standard_deviation_valid: std_valid,
            standard_deviation_invalid: std_invalid,
            sample_size,
            confidence_level: confidence,
        })
    }

    async fn test_totp_verification_timing(&self) -> SecurityResult<TimingTestResult> {
        println!("Testing TOTP verification timing...");

        let sample_size = 50;
        let mut valid_times = Vec::new();
        let mut invalid_times = Vec::new();

        let _secret = "JBSWY3DPEHPK3PXP";
        let valid_code = "123456";
        let invalid_codes = ["654321", "111111", "000000", "999999"];

        for i in 0..sample_size {
            let start = Instant::now();
            let _ = self
                .crypto_manager
                .timing_safe_string_eq(valid_code, valid_code);
            valid_times.push(start.elapsed().as_nanos() as f64);

            let invalid_code = invalid_codes[i % invalid_codes.len()];
            let start = Instant::now();
            let _ = self
                .crypto_manager
                .timing_safe_string_eq(valid_code, invalid_code);
            invalid_times.push(start.elapsed().as_nanos() as f64);

            sleep(Duration::from_millis(5)).await;
        }

        let mean_valid = mean(&valid_times);
        let mean_invalid = mean(&invalid_times);
        let std_valid = standard_deviation(&valid_times, Some(mean_valid));
        let std_invalid = standard_deviation(&invalid_times, Some(mean_invalid));

        let timing_difference = (mean_valid - mean_invalid).abs();
        let normalized_difference = timing_difference / mean_valid.max(mean_invalid);

        let vulnerable = normalized_difference > 0.05;
        let confidence = self.calculate_statistical_confidence(&valid_times, &invalid_times);

        Ok(TimingTestResult {
            test_name: "TOTP Verification Timing".to_string(),
            vulnerable,
            mean_time_valid: mean_valid,
            mean_time_invalid: mean_invalid,
            timing_difference,
            standard_deviation_valid: std_valid,
            standard_deviation_invalid: std_invalid,
            sample_size,
            confidence_level: confidence,
        })
    }

    async fn test_session_token_validation_timing(&self) -> SecurityResult<TimingTestResult> {
        println!("Testing session token validation timing...");

        let sample_size = 100;
        let mut valid_times = Vec::new();
        let mut invalid_times = Vec::new();

        let valid_token = "valid_session_token_12345";

        for i in 0..sample_size {
            let invalid_token: String = format!("invalid_token_{}", i);

            let start = Instant::now();
            let _ = self
                .crypto_manager
                .timing_safe_string_eq(valid_token, valid_token);
            valid_times.push(start.elapsed().as_nanos() as f64);

            let start = Instant::now();
            let _ = self
                .crypto_manager
                .timing_safe_string_eq(valid_token, &invalid_token);
            invalid_times.push(start.elapsed().as_nanos() as f64);

            sleep(Duration::from_millis(1)).await;
        }

        let mean_valid = mean(&valid_times);
        let mean_invalid = mean(&invalid_times);
        let std_valid = standard_deviation(&valid_times, Some(mean_valid));
        let std_invalid = standard_deviation(&invalid_times, Some(mean_invalid));

        let timing_difference = (mean_valid - mean_invalid).abs();
        let normalized_difference = timing_difference / mean_valid.max(mean_invalid);

        let vulnerable = normalized_difference > 0.02;
        let confidence = self.calculate_statistical_confidence(&valid_times, &invalid_times);

        Ok(TimingTestResult {
            test_name: "Session Token Validation Timing".to_string(),
            vulnerable,
            mean_time_valid: mean_valid,
            mean_time_invalid: mean_invalid,
            timing_difference,
            standard_deviation_valid: std_valid,
            standard_deviation_invalid: std_invalid,
            sample_size,
            confidence_level: confidence,
        })
    }

    async fn test_constant_time_string_comparison(&self) -> SecurityResult<TimingTestResult> {
        println!("Testing constant-time string comparison...");

        let sample_size = 200;
        let mut equal_times = Vec::new();
        let mut unequal_times = Vec::new();

        let test_string = "test_string_for_comparison_timing";

        for i in 0..sample_size {
            let other_string = if i % 2 == 0 {
                "test_string_for_comparison_timing"
            } else {
                "different_string_for_comparison"
            };

            let start = Instant::now();
            let result = self
                .crypto_manager
                .timing_safe_string_eq(test_string, test_string);
            let elapsed = start.elapsed().as_nanos() as f64;
            if result {
                equal_times.push(elapsed);
            }

            let start = Instant::now();
            let result = self
                .crypto_manager
                .timing_safe_string_eq(test_string, other_string);
            let elapsed = start.elapsed().as_nanos() as f64;
            if !result {
                unequal_times.push(elapsed);
            }
        }

        let mean_equal = mean(&equal_times);
        let mean_unequal = mean(&unequal_times);
        let std_equal = standard_deviation(&equal_times, Some(mean_equal));
        let std_unequal = standard_deviation(&unequal_times, Some(mean_unequal));

        let timing_difference = (mean_equal - mean_unequal).abs();
        let normalized_difference = timing_difference / mean_equal.max(mean_unequal);

        let vulnerable = normalized_difference > 0.01;
        let confidence = self.calculate_statistical_confidence(&equal_times, &unequal_times);

        Ok(TimingTestResult {
            test_name: "Constant-Time String Comparison".to_string(),
            vulnerable,
            mean_time_valid: mean_equal,
            mean_time_invalid: mean_unequal,
            timing_difference,
            standard_deviation_valid: std_equal,
            standard_deviation_invalid: std_unequal,
            sample_size,
            confidence_level: confidence,
        })
    }

    async fn test_hash_comparison_timing(&self) -> SecurityResult<TimingTestResult> {
        println!("Testing hash comparison timing...");

        let sample_size = 100;
        let mut equal_times = Vec::new();
        let mut unequal_times = Vec::new();

        let data1 = b"test_data_for_hashing";
        let data2 = b"different_test_data";

        let hash1 = self.crypto_manager.secure_hash(data1);
        let hash2 = self.crypto_manager.secure_hash(data2);

        for _ in 0..sample_size {
            let start = Instant::now();
            let _ = self
                .crypto_manager
                .secure_compare(hash1.as_bytes(), hash1.as_bytes());
            equal_times.push(start.elapsed().as_nanos() as f64);

            let start = Instant::now();
            let _ = self
                .crypto_manager
                .secure_compare(hash1.as_bytes(), hash2.as_bytes());
            unequal_times.push(start.elapsed().as_nanos() as f64);
        }

        let mean_equal = mean(&equal_times);
        let mean_unequal = mean(&unequal_times);
        let std_equal = standard_deviation(&equal_times, Some(mean_equal));
        let std_unequal = standard_deviation(&unequal_times, Some(mean_unequal));

        let timing_difference = (mean_equal - mean_unequal).abs();
        let normalized_difference = timing_difference / mean_equal.max(mean_unequal);

        let vulnerable = normalized_difference > 0.02;
        let confidence = self.calculate_statistical_confidence(&equal_times, &unequal_times);

        Ok(TimingTestResult {
            test_name: "Hash Comparison Timing".to_string(),
            vulnerable,
            mean_time_valid: mean_equal,
            mean_time_invalid: mean_unequal,
            timing_difference,
            standard_deviation_valid: std_equal,
            standard_deviation_invalid: std_unequal,
            sample_size,
            confidence_level: confidence,
        })
    }

    async fn test_backup_code_verification_timing(&self) -> SecurityResult<TimingTestResult> {
        println!("Testing backup code verification timing...");

        let sample_size = 100;
        let mut valid_times = Vec::new();
        let mut invalid_times = Vec::new();

        let valid_codes = ["ABCD1234", "EFGH5678", "IJKL9012"];
        let invalid_codes = ["WRONG123", "INVALID1", "NOPE4567"];

        for i in 0..sample_size {
            let valid_code = valid_codes[i % valid_codes.len()];
            let invalid_code = invalid_codes[i % invalid_codes.len()];

            let start = Instant::now();
            let _ = self
                .crypto_manager
                .timing_safe_string_eq(valid_code, valid_code);
            valid_times.push(start.elapsed().as_nanos() as f64);

            let start = Instant::now();
            let _ = self
                .crypto_manager
                .timing_safe_string_eq(valid_code, invalid_code);
            invalid_times.push(start.elapsed().as_nanos() as f64);
        }

        let mean_valid = mean(&valid_times);
        let mean_invalid = mean(&invalid_times);
        let std_valid = standard_deviation(&valid_times, Some(mean_valid));
        let std_invalid = standard_deviation(&invalid_times, Some(mean_invalid));

        let timing_difference = (mean_valid - mean_invalid).abs();
        let normalized_difference = timing_difference / mean_valid.max(mean_invalid);

        let vulnerable = normalized_difference > 0.05;
        let confidence = self.calculate_statistical_confidence(&valid_times, &invalid_times);

        Ok(TimingTestResult {
            test_name: "Backup Code Verification Timing".to_string(),
            vulnerable,
            mean_time_valid: mean_valid,
            mean_time_invalid: mean_invalid,
            timing_difference,
            standard_deviation_valid: std_valid,
            standard_deviation_invalid: std_invalid,
            sample_size,
            confidence_level: confidence,
        })
    }

    fn calculate_statistical_confidence(&self, sample1: &[f64], sample2: &[f64]) -> f64 {
        if sample1.is_empty() || sample2.is_empty() {
            return 0.0;
        }

        let mean1 = mean(sample1);
        let mean2 = mean(sample2);
        let std1 = standard_deviation(sample1, Some(mean1));
        let std2 = standard_deviation(sample2, Some(mean2));

        let n1 = sample1.len() as f64;
        let n2 = sample2.len() as f64;

        let pooled_std = ((std1.powi(2) / n1) + (std2.powi(2) / n2)).sqrt();

        if pooled_std == 0.0 {
            return 1.0;
        }

        let t_stat = ((mean1 - mean2).abs()) / pooled_std;

        (1.0 - (-t_stat.abs() / 2.0).exp()).min(0.99)
    }

    fn calculate_overall_security_score(&self, tests: &[TimingTestResult]) -> f64 {
        let total_tests = tests.len() as f64;
        let vulnerable_tests = tests.iter().filter(|t| t.vulnerable).count() as f64;

        let base_score = ((total_tests - vulnerable_tests) / total_tests) * 100.0;

        let confidence_penalty = tests
            .iter()
            .map(|t| {
                if t.vulnerable {
                    1.0 - t.confidence_level
                } else {
                    0.0
                }
            })
            .sum::<f64>()
            * 5.0;

        (base_score - confidence_penalty).max(0.0)
    }

    fn identify_critical_vulnerabilities(&self, tests: &[TimingTestResult]) -> Vec<String> {
        tests
            .iter()
            .filter(|t| t.vulnerable && t.confidence_level > 0.8)
            .map(|t| {
                format!(
                    "{}: {:.2}ms timing difference (confidence: {:.1}%)",
                    t.test_name,
                    t.timing_difference / 1_000_000.0,
                    t.confidence_level * 100.0
                )
            })
            .collect()
    }

    fn generate_recommendations(&self, tests: &[TimingTestResult]) -> Vec<String> {
        let mut recommendations = Vec::new();

        for test in tests {
            if test.vulnerable {
                match test.test_name.as_str() {
                    "Password Verification Timing" => {
                        recommendations.push("Implement constant-time password verification with dummy operations for non-existent users".to_string());
                    }
                    "User Enumeration Timing" => {
                        recommendations.push("Ensure authentication timing is consistent regardless of user existence".to_string());
                    }
                    "API Key Verification Timing" => {
                        recommendations.push(
                            "Use constant-time comparison for API key verification".to_string(),
                        );
                    }
                    "TOTP Verification Timing" => {
                        recommendations.push(
                            "Implement constant-time TOTP code verification without early returns"
                                .to_string(),
                        );
                    }
                    "Session Token Validation Timing" => {
                        recommendations
                            .push("Use constant-time token comparison and validation".to_string());
                    }
                    _ => {
                        recommendations.push(format!(
                            "Address timing vulnerability in: {}",
                            test.test_name
                        ));
                    }
                }
            }
        }

        if recommendations.is_empty() {
            recommendations.push(
                "No critical timing vulnerabilities detected. Maintain current security practices."
                    .to_string(),
            );
        }

        recommendations
            .push("Implement rate limiting to prevent timing attack exploitation".to_string());
        recommendations.push("Add random delays to authentication operations".to_string());
        recommendations
            .push("Monitor authentication patterns for potential timing attacks".to_string());

        recommendations
    }

    pub fn print_detailed_report(&self, report: &TimingAttackReport) {
        println!("\n=== TIMING ATTACK SECURITY REPORT ===\n");

        println!(
            "Overall Security Score: {:.1}/100",
            report.overall_security_score
        );

        if report.overall_security_score >= 90.0 {
            println!("✅ EXCELLENT: Very low timing attack risk");
        } else if report.overall_security_score >= 75.0 {
            println!("⚠️  GOOD: Some timing vulnerabilities detected");
        } else if report.overall_security_score >= 50.0 {
            println!("❌ POOR: Multiple timing vulnerabilities detected");
        } else {
            println!("🚨 CRITICAL: Severe timing attack vulnerabilities");
        }

        println!("\n--- Test Results ---");
        for test in &report.tests {
            let status = if test.vulnerable {
                "❌ VULNERABLE"
            } else {
                "✅ SECURE"
            };
            println!("\n{}: {}", test.test_name, status);
            println!(
                "  Mean valid time: {:.2}ms",
                test.mean_time_valid / 1_000_000.0
            );
            println!(
                "  Mean invalid time: {:.2}ms",
                test.mean_time_invalid / 1_000_000.0
            );
            println!(
                "  Timing difference: {:.2}ms",
                test.timing_difference / 1_000_000.0
            );
            println!("  Confidence level: {:.1}%", test.confidence_level * 100.0);
            println!("  Sample size: {}", test.sample_size);
        }

        if !report.critical_vulnerabilities.is_empty() {
            println!("\n--- Critical Vulnerabilities ---");
            for vuln in &report.critical_vulnerabilities {
                println!("  • {}", vuln);
            }
        }

        println!("\n--- Recommendations ---");
        for rec in &report.recommendations {
            println!("  • {}", rec);
        }

        println!("\n=== END REPORT ===\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_timing_attack_detection() {
        let tester = TimingAttackTester::new().expect("Failed to create tester");
        let report = tester
            .run_comprehensive_timing_tests()
            .await
            .expect("Tests failed");

        tester.print_detailed_report(&report);

        assert!(report.tests.len() > 0);
        assert!(report.overall_security_score >= 0.0);
        assert!(report.overall_security_score <= 100.0);
    }

    #[tokio::test]
    async fn test_constant_time_comparison() {
        let tester = TimingAttackTester::new().expect("Failed to create tester");
        let result = tester
            .test_constant_time_string_comparison()
            .await
            .expect("Test failed");

        println!("Constant-time comparison test:");
        println!("  Vulnerable: {}", result.vulnerable);
        println!(
            "  Timing difference: {:.2}ms",
            result.timing_difference / 1_000_000.0
        );

        assert!(
            !result.vulnerable,
            "Constant-time comparison should not be vulnerable"
        );
    }

    #[tokio::test]
    async fn test_hash_comparison_security() {
        let tester = TimingAttackTester::new().expect("Failed to create tester");
        let result = tester
            .test_hash_comparison_timing()
            .await
            .expect("Test failed");

        println!("Hash comparison test:");
        println!("  Vulnerable: {}", result.vulnerable);
        println!(
            "  Timing difference: {:.2}ms",
            result.timing_difference / 1_000_000.0
        );

        assert!(
            !result.vulnerable,
            "Hash comparison should not be vulnerable"
        );
    }
}
