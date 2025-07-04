# Chaos Test Fixes for Lightning DB

## Issues Found

### 1. DiskFull Test - Not Actually Triggering Disk Full
**Problem**: The test creates files on disk but doesn't actually exhaust available space effectively.

**Root Cause**: 
- Modern filesystems have too much space available
- Test only fills 100MB which isn't enough
- Database may be buffering writes in memory/cache

**Solution**: Create a more aggressive disk full simulation

### 2. FilePermissions Test - Database Can't Handle Read-Only Files
**Problem**: Database fails to open when files are read-only, but should gracefully degrade.

**Root Cause**: 
- Database tries to open files in read-write mode even for read operations
- No fallback to read-only mode when write permissions are denied

**Solution**: Implement graceful degradation for permission issues

## Implementation Plan

### Fix 1: Enhanced DiskFull Test
```rust
// Use a small tmpfs or quota-limited filesystem for more reliable testing
// Or fill disk more aggressively with larger files
// Check for actual ENOSPC errors from the OS
```

### Fix 2: Database Read-Only Mode Support
The database needs to:
1. Detect permission errors during file opening
2. Fall back to read-only mode automatically
3. Return appropriate errors for write operations when in read-only mode
4. Allow reads to continue working

### Fix 3: Better Error Handling in Storage Layer
- Improve error propagation for disk space issues
- Add specific error types for resource exhaustion
- Implement proper fallback modes

## Test Improvements Needed

1. **More Realistic Disk Full**: Use smaller temporary filesystems or more aggressive filling
2. **Graceful Permission Handling**: Database should handle read-only files better
3. **Better Error Classification**: Distinguish between recoverable and unrecoverable errors
4. **Recovery Testing**: Test that database recovers when resources become available

## Expected Outcomes After Fixes

- **DiskFull**: Database should handle ENOSPC gracefully, preserve existing data
- **FilePermissions**: Database should open in read-only mode, allow reads but reject writes
- **Overall**: >95% chaos test pass rate

## Critical for Production

These scenarios represent real-world failures that will happen in production:
- Disk space exhaustion
- Permission changes during deployment/maintenance
- Resource constraints

The database must handle these gracefully without corruption or crashes.