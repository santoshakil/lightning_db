#!/bin/bash
# Script to fix common clippy warnings in examples

# Find all examples with the pattern
files=$(find examples -name "*.rs" -exec grep -l "let mut config = LightningDbConfig::default();" {} \;)

for file in $files; do
    echo "Processing $file..."
    
    # Check if there's only one field assignment after the config
    assignments=$(grep -A5 "let mut config = LightningDbConfig::default();" "$file" | grep "config\." | wc -l)
    
    if [ "$assignments" -eq 1 ]; then
        # Get the field assignment
        field_line=$(grep -A5 "let mut config = LightningDbConfig::default();" "$file" | grep "config\." | head -1)
        field=$(echo "$field_line" | sed -E 's/.*config\.([a-z_]+).*/\1/')
        value=$(echo "$field_line" | sed -E 's/.*= (.*);/\1/')
        
        # Create the replacement
        echo "  Field: $field = $value"
        
        # Use perl for multi-line replacement
        perl -i -pe 'BEGIN{undef $/;} s/let mut config = LightningDbConfig::default\(\);\s*config\.'$field' = '$value';/let config = LightningDbConfig {\n        '$field': '$value',\n        ..Default::default()\n    };/smg' "$file"
    fi
done

echo "Done fixing clippy warnings"