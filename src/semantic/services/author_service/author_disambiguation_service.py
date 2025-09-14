#!/usr/bin/env python3
"""
Author Disambiguation Service
Implements multi-level author matching strategies for DBLP and Semantic Scholar data
"""

import re
from typing import List, Dict, Tuple
from thefuzz import fuzz
from unidecode import unidecode


class AuthorMatcher:
    """Advanced author disambiguation and matching system"""
    
    def __init__(self):
        self.match_stats = {
            'exact_matches': 0,
            'initialism_matches': 0, 
            'structural_matches': 0,
            'fuzzy_matches': 0,
            'unique_initial_matches': 0,
            'positional_matches': 0,
            'unmatched': 0
        }
    
    def normalize_name(self, name: str) -> str:
        """
        Comprehensive name normalization for cross-platform matching
        Handles: numeric suffixes, special characters, abbreviations, punctuation
        
        Args:
            name: Raw author name string
            
        Returns:
            Normalized name string
        """
        if not name or not isinstance(name, str):
            return ""
        
        # Handle "LastName, FirstName" format
        if ',' in name:
            parts = name.split(',', 1)
            if len(parts) == 2:
                name = f"{parts[1].strip()} {parts[0].strip()}"
        
        # Remove DBLP numeric suffixes (e.g., "0001", "0004") - CRITICAL FIX
        name = re.sub(r'\s+\d{4}$', '', name)
        
        # Remove other numeric disambiguation patterns
        name = re.sub(r'\s+\d{1,3}$', '', name)  # Handle 1-3 digit suffixes
        
        # Unicode normalization and lowercase
        name = unidecode(name).lower()
        
        # Remove common academic suffixes and titles
        suffixes = ['jr', 'sr', 'phd', 'md', 'iii', 'ii', 'iv', 'v', 'esq', 'dr', 'prof', 'professor']
        name_parts = name.split()
        filtered_parts = [part for part in name_parts if part not in suffixes]
        name = ' '.join(filtered_parts)
        
        # Standardize punctuation and whitespace
        name = name.replace('-', ' ')
        name = name.replace('_', ' ')
        
        # Remove all punctuation except dots (for initials)
        name = re.sub(r'[^\w\s\.]', '', name)
        
        # Normalize multiple spaces
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def get_name_interpretations(self, normalized_name: str) -> List[Tuple[str, str]]:
        """
        Generate possible interpretations of a name as (first_initial, last_name)
        
        Args:
            normalized_name: Normalized name string
            
        Returns:
            List of (first_initial, last_name) tuples
        """
        parts = normalized_name.split()
        
        if len(parts) >= 3:
            # Multi-part name: assume last part is surname
            first_initial = parts[0][0] if parts[0] else ''
            last_name = parts[-1]
            return [(first_initial, last_name)]
        
        elif len(parts) == 2:
            # Two-part name: generate both interpretations
            return [
                (parts[0][0] if parts[0] else '', parts[1]),  # First Last
                (parts[1][0] if parts[1] else '', parts[0])   # Last First
            ]
        
        elif len(parts) == 1:
            # Single part: assume it's the last name
            return [('', parts[0])]
        
        return []
    
    def match_authors_enhanced(self, dblp_authors: List[str], s2_authors: List[Dict]) -> Tuple[Dict, List]:
        """
        Enhanced multi-tier author matching algorithm
        
        Args:
            dblp_authors: List of DBLP author name strings
            s2_authors: List of S2 author objects with 'name' and 'authorId'
            
        Returns:
            Tuple of (matched_pairs_dict, unmatched_dblp_authors)
        """
        if not dblp_authors or not s2_authors:
            return {}, dblp_authors
        
        # Filter valid S2 authors
        valid_s2_authors = [
            author for author in s2_authors 
            if author.get('name') and str(author['name']).strip()
        ]
        
        if not valid_s2_authors:
            return {}, dblp_authors
        
        # Normalize all names
        dblp_normalized = {self.normalize_name(name): name for name in dblp_authors}
        s2_normalized = {self.normalize_name(author['name']): author for author in valid_s2_authors}
        
        # Initialize tracking sets
        matched = {}
        remaining_dblp = set(dblp_normalized.keys())
        remaining_s2 = set(s2_normalized.keys())
        
        # Tier 1: Exact full name matching
        exact_matches = remaining_dblp.intersection(remaining_s2)
        for norm_name in exact_matches:
            matched[dblp_normalized[norm_name]] = s2_normalized[norm_name]
            remaining_dblp.remove(norm_name)
            remaining_s2.remove(norm_name)
            self.match_stats['exact_matches'] += 1
        
        # Tier 2: Enhanced initialism matching (F. Last <-> FirstName Last)
        for dblp_norm in list(remaining_dblp):
            dblp_parts = dblp_norm.split()
            if len(dblp_parts) >= 2:
                # Create multiple initialism patterns
                dblp_patterns = []
                
                # Pattern 1: F Last (first initial + last name)
                dblp_patterns.append(''.join(part[0] for part in dblp_parts[:-1]) + ' ' + dblp_parts[-1])
                
                # Pattern 2: F. Last (with dots)
                dblp_patterns.append('.'.join(part[0] for part in dblp_parts[:-1]) + '. ' + dblp_parts[-1])
                
                # Pattern 3: Handle middle initials (F M Last -> F. M. Last)
                if len(dblp_parts) > 2:
                    dblp_patterns.append(' '.join(part[0] + '.' for part in dblp_parts[:-1]) + ' ' + dblp_parts[-1])
                
                for s2_norm in list(remaining_s2):
                    s2_parts = s2_norm.split()
                    if len(s2_parts) >= 2:
                        # Create S2 patterns
                        s2_patterns = []
                        s2_patterns.append(''.join(part[0] for part in s2_parts[:-1]) + ' ' + s2_parts[-1])
                        s2_patterns.append('.'.join(part[0] for part in s2_parts[:-1]) + '. ' + s2_parts[-1])
                        if len(s2_parts) > 2:
                            s2_patterns.append(' '.join(part[0] + '.' for part in s2_parts[:-1]) + ' ' + s2_parts[-1])
                        
                        # Cross-match all patterns
                        pattern_matched = False
                        for dblp_pattern in dblp_patterns:
                            for s2_pattern in s2_patterns:
                                if dblp_pattern == s2_pattern:
                                    matched[dblp_normalized[dblp_norm]] = s2_normalized[s2_norm]
                                    remaining_dblp.remove(dblp_norm)
                                    remaining_s2.remove(s2_norm)
                                    self.match_stats['initialism_matches'] += 1
                                    pattern_matched = True
                                    break
                            if pattern_matched:
                                break
                        if pattern_matched:
                            break
        
        # Tier 3: Structural interpretation matching
        for dblp_norm in list(remaining_dblp):
            dblp_interpretations = self.get_name_interpretations(dblp_norm)
            
            for s2_norm in list(remaining_s2):
                s2_interpretations = self.get_name_interpretations(s2_norm)
                
                # Check if any interpretations match
                matched_interpretation = False
                for dblp_interp in dblp_interpretations:
                    for s2_interp in s2_interpretations:
                        if dblp_interp == s2_interp and dblp_interp != ('', ''):
                            matched[dblp_normalized[dblp_norm]] = s2_normalized[s2_norm]
                            remaining_dblp.remove(dblp_norm)
                            remaining_s2.remove(s2_norm)
                            self.match_stats['structural_matches'] += 1
                            matched_interpretation = True
                            break
                    if matched_interpretation:
                        break
                if matched_interpretation:
                    break
        
        # Tier 4: Enhanced fuzzy string matching with multiple algorithms
        for dblp_norm in list(remaining_dblp):
            best_match = None
            best_score = 0
            
            for s2_norm in list(remaining_s2):
                # Use multiple fuzzy matching algorithms
                token_sort_score = fuzz.token_sort_ratio(dblp_norm, s2_norm)
                token_set_score = fuzz.token_set_ratio(dblp_norm, s2_norm)
                partial_score = fuzz.partial_ratio(dblp_norm, s2_norm)
                
                # Take the maximum of different algorithms
                combined_score = max(token_sort_score, token_set_score, partial_score)
                
                # Lower threshold for better recall, but still high enough for precision
                if combined_score > 85 and combined_score > best_score:
                    best_score = combined_score
                    best_match = s2_norm
            
            if best_match:
                matched[dblp_normalized[dblp_norm]] = s2_normalized[best_match]
                remaining_dblp.remove(dblp_norm)
                remaining_s2.remove(best_match)
                self.match_stats['fuzzy_matches'] += 1
        
        # Tier 5: Abbreviation and initial variants matching
        for dblp_norm in list(remaining_dblp):
            matched_variant = False  # Move this outside the inner loop
            
            for s2_norm in list(remaining_s2):
                # Check if one is an abbreviation of the other
                if self._is_abbreviation_match(dblp_norm, s2_norm):
                    matched[dblp_normalized[dblp_norm]] = s2_normalized[s2_norm]
                    remaining_dblp.remove(dblp_norm)
                    remaining_s2.remove(s2_norm)
                    self.match_stats['initialism_matches'] += 1
                    matched_variant = True
                    break
            
            if matched_variant:
                break
        
        # Tier 6: Unique initial matching for single letters
        for dblp_norm in list(remaining_dblp):
            dblp_parts = dblp_norm.split()
            if len(dblp_parts) == 1 and len(dblp_parts[0]) == 1:
                initial = dblp_parts[0]
                matching_s2 = []
                
                for s2_norm in remaining_s2:
                    s2_parts = s2_norm.split()
                    if s2_parts and s2_parts[0].startswith(initial):
                        matching_s2.append(s2_norm)
                
                if len(matching_s2) == 1:
                    matched[dblp_normalized[dblp_norm]] = s2_normalized[matching_s2[0]]
                    remaining_dblp.remove(dblp_norm)
                    remaining_s2.remove(matching_s2[0])
                    self.match_stats['unique_initial_matches'] += 1
        
        # Tier 7: Positional fallback (last resort)
        if len(remaining_dblp) == 1 and len(remaining_s2) == 1:
            dblp_norm = list(remaining_dblp)[0]
            s2_norm = list(remaining_s2)[0]
            matched[dblp_normalized[dblp_norm]] = s2_normalized[s2_norm]
            remaining_dblp.clear()
            remaining_s2.clear()
            self.match_stats['positional_matches'] += 1
        
        # Count unmatched
        self.match_stats['unmatched'] += len(remaining_dblp)
        
        # Return unmatched DBLP authors
        unmatched = [dblp_normalized[norm] for norm in remaining_dblp]
        
        return matched, unmatched
    
    def _is_abbreviation_match(self, name1: str, name2: str) -> bool:
        """
        Check if one name is an abbreviation variant of another
        Handles cases like: "j. smith" <-> "john smith", "e. yamamoto" <-> "eiko yamamoto"
        """
        parts1 = name1.split()
        parts2 = name2.split()
        
        if len(parts1) != len(parts2):
            return False
        
        for p1, p2 in zip(parts1, parts2):
            # Remove dots for comparison
            p1_clean = p1.replace('.', '')
            p2_clean = p2.replace('.', '')
            
            # If both are single characters, they should match
            if len(p1_clean) == 1 and len(p2_clean) == 1:
                if p1_clean != p2_clean:
                    return False
            # If one is single char and other is full word, check initial
            elif len(p1_clean) == 1:
                if p1_clean != p2_clean[0]:
                    return False
            elif len(p2_clean) == 1:
                if p2_clean != p1_clean[0]:
                    return False
            # If both are full words, they should be exactly the same
            else:
                if p1_clean != p2_clean:
                    return False
        
        return True
    
    def get_match_statistics(self) -> Dict:
        """Get comprehensive matching statistics"""
        total_attempts = sum(self.match_stats.values())
        
        stats_with_percentages = {}
        for key, count in self.match_stats.items():
            stats_with_percentages[key] = {
                'count': count,
                'percentage': (count / total_attempts * 100) if total_attempts > 0 else 0
            }
        
        return {
            'total_match_attempts': total_attempts,
            'detailed_stats': stats_with_percentages,
            'success_rate': ((total_attempts - self.match_stats['unmatched']) / total_attempts * 100) if total_attempts > 0 else 0
        }