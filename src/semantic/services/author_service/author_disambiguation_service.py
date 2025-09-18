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
            'prefix_matches': 0,          # New
            'nickname_matches': 0,        # New
            'flexible_abbrev_matches': 0, # New
            'unmatched': 0
        }

        # Comprehensive nickname mapping
        self.nickname_map = {
            # Common English nicknames
            'bob': ['robert', 'bobby'],
            'bill': ['william', 'billy', 'will'],
            'dick': ['richard', 'rick', 'ricky'],
            'tom': ['thomas', 'tommy'],
            'jim': ['james', 'jimmy'],
            'mike': ['michael', 'mickey'],
            'dave': ['david', 'davy'],
            'chris': ['christopher', 'christina', 'christine'],
            'steve': ['steven', 'stephen'],
            'joe': ['joseph', 'joey'],
            'sam': ['samuel', 'samantha'],
            'alex': ['alexander', 'alexandra', 'alexis'],
            'nick': ['nicholas', 'nicole'],
            'pat': ['patrick', 'patricia'],
            'matt': ['matthew', 'matthias'],
            'dan': ['daniel', 'danny'],
            'rob': ['robert', 'robbie'],
            'andy': ['andrew', 'andreas'],
            'tony': ['anthony', 'antonio'],
            'ben': ['benjamin', 'benedict'],
            # International variations
            'seb': ['sebastian', 'sebastien'],
            'max': ['maximilian', 'maximiliano', 'maxime'],
            'fred': ['frederick', 'frederic'],
            'beth': ['elizabeth', 'bethany'],
            'liz': ['elizabeth', 'lizzy'],
            'kate': ['katherine', 'kathryn', 'katie'],
            'sue': ['susan', 'susanne'],
        }

        # Create reverse mapping for efficiency
        self.reverse_nickname_map = {}
        for nickname, full_names in self.nickname_map.items():
            for full_name in full_names:
                if full_name not in self.reverse_nickname_map:
                    self.reverse_nickname_map[full_name] = []
                self.reverse_nickname_map[full_name].append(nickname)
    
    def _enhanced_comma_processing(self, name: str) -> str:
        """
        Enhanced comma processing for complex name formats
        Handles multiple comma patterns and edge cases
        """
        if not ',' in name:
            return name

        # Split by comma and clean parts
        parts = [part.strip() for part in name.split(',')]

        # Handle different comma patterns
        if len(parts) == 2:
            # Simple "Last, First" or "Last, First Middle"
            last_part = parts[0]
            first_part = parts[1]
            return f"{first_part} {last_part}"

        elif len(parts) == 3:
            # "Last, First, Jr." or "Last, First Middle, Suffix"
            last_part = parts[0]
            first_part = parts[1]
            suffix = parts[2].lower()

            # Common suffixes to move to end
            if suffix in ['jr', 'sr', 'iii', 'ii', 'iv', 'v', 'phd', 'md', 'esq']:
                return f"{first_part} {last_part} {suffix}"
            else:
                # Treat as additional first/middle name
                return f"{first_part} {suffix} {last_part}"

        elif len(parts) > 3:
            # Complex case: "Last, First, Middle, Suffix"
            last_part = parts[0]
            first_parts = parts[1:-1]  # Everything except last and first
            potential_suffix = parts[-1].lower()

            if potential_suffix in ['jr', 'sr', 'iii', 'ii', 'iv', 'v', 'phd', 'md', 'esq']:
                # Last part is suffix
                return f"{' '.join(first_parts)} {last_part} {potential_suffix}"
            else:
                # All middle parts are names
                return f"{' '.join(parts[1:])} {last_part}"

        # Fallback: join all parts
        return ' '.join(parts)

    def normalize_name(self, name: str) -> str:
        """
        Enhanced comprehensive name normalization for cross-platform matching
        Handles: numeric suffixes, special characters, abbreviations, punctuation, complex comma formats

        Args:
            name: Raw author name string

        Returns:
            Normalized name string
        """
        if not name or not isinstance(name, str):
            return ""

        # Enhanced comma handling
        name = self._enhanced_comma_processing(name)

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
        ENHANCED multi-tier author matching algorithm with advanced matching strategies

        New Features:
        - Flexible abbreviation matching: "R. Feris" <-> "Rogério Feris"
        - Nickname matching: "Bob Smith" <-> "Robert Smith"
        - Prefix/truncation matching: "Andr" <-> "André", "Seb" <-> "Sébastien"
        - Enhanced comma processing: "Jean, Sébastien" <-> "Sébastien Jean"

        Matching Tiers:
        1. Exact full name matching
        2. Enhanced initialism matching
        2.5. Flexible abbreviation matching (NEW)
        2.7. Nickname matching (NEW)
        3. Structural interpretation matching
        3.5. Prefix/truncation matching (NEW)
        4. Enhanced fuzzy string matching
        5. Enhanced abbreviation variants
        6. Unique initial matching
        7. Positional fallback

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

        # Tier 2.5: NEW - Flexible abbreviation matching
        for dblp_norm in list(remaining_dblp):
            matched_flexible = False
            for s2_norm in list(remaining_s2):
                if self._flexible_abbreviation_match(dblp_norm, s2_norm):
                    matched[dblp_normalized[dblp_norm]] = s2_normalized[s2_norm]
                    remaining_dblp.remove(dblp_norm)
                    remaining_s2.remove(s2_norm)
                    self.match_stats['flexible_abbrev_matches'] += 1
                    matched_flexible = True
                    break
            if matched_flexible:
                continue

        # Tier 2.7: NEW - Nickname matching
        for dblp_norm in list(remaining_dblp):
            matched_nickname = False
            for s2_norm in list(remaining_s2):
                if self._nickname_match(dblp_norm, s2_norm):
                    matched[dblp_normalized[dblp_norm]] = s2_normalized[s2_norm]
                    remaining_dblp.remove(dblp_norm)
                    remaining_s2.remove(s2_norm)
                    self.match_stats['nickname_matches'] += 1
                    matched_nickname = True
                    break
            if matched_nickname:
                continue

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

        # Tier 3.5: NEW - Prefix/truncation matching
        for dblp_norm in list(remaining_dblp):
            best_prefix_match = None
            best_prefix_score = 0

            for s2_norm in list(remaining_s2):
                prefix_score = self._enhanced_prefix_matching(dblp_norm, s2_norm)
                if prefix_score > 75 and prefix_score > best_prefix_score:  # Threshold for prefix matching
                    best_prefix_score = prefix_score
                    best_prefix_match = s2_norm

            if best_prefix_match:
                matched[dblp_normalized[dblp_norm]] = s2_normalized[best_prefix_match]
                remaining_dblp.remove(dblp_norm)
                remaining_s2.remove(best_prefix_match)
                self.match_stats['prefix_matches'] += 1

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
        
        # Tier 5: Enhanced abbreviation variants (using improved flexible matching)
        for dblp_norm in list(remaining_dblp):
            matched_variant = False
            for s2_norm in list(remaining_s2):
                # Use improved flexible abbreviation method
                if self._flexible_abbreviation_match(dblp_norm, s2_norm):
                    matched[dblp_normalized[dblp_norm]] = s2_normalized[s2_norm]
                    remaining_dblp.remove(dblp_norm)
                    remaining_s2.remove(s2_norm)
                    self.match_stats['flexible_abbrev_matches'] += 1
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

    def _get_name_variants(self, name: str) -> List[str]:
        """
        Generate all possible variants of a name including nicknames
        """
        variants = [name]  # Original name
        name_lower = name.lower()

        # Add nickname variants
        if name_lower in self.nickname_map:
            variants.extend(self.nickname_map[name_lower])

        # Add reverse nickname variants
        if name_lower in self.reverse_nickname_map:
            variants.extend(self.reverse_nickname_map[name_lower])

        return list(set(variants))  # Remove duplicates

    def _flexible_abbreviation_match(self, name1: str, name2: str) -> bool:
        """
        Enhanced abbreviation matching for different length name parts
        Handles: "R. Feris" <-> "Rogério Feris", "J. Smith" <-> "John Smith"
        """
        parts1 = name1.split()
        parts2 = name2.split()

        # Try both directions: name1 as abbreviation of name2, and vice versa
        return (self._is_abbreviation_of(parts1, parts2) or
                self._is_abbreviation_of(parts2, parts1))

    def _is_abbreviation_of(self, short_parts: List[str], long_parts: List[str]) -> bool:
        """Check if short_parts is an abbreviation of long_parts"""
        if len(short_parts) > len(long_parts):
            return False

        # Last name must match exactly
        if not short_parts or not long_parts:
            return False

        short_last = short_parts[-1].replace('.', '')
        long_last = long_parts[-1].replace('.', '')
        if short_last != long_last:
            return False

        # Check first names (all parts except last)
        short_first_parts = short_parts[:-1]
        long_first_parts = long_parts[:-1]

        if len(short_first_parts) > len(long_first_parts):
            return False

        for i, short_part in enumerate(short_first_parts):
            if i >= len(long_first_parts):
                return False

            short_clean = short_part.replace('.', '')
            long_clean = long_first_parts[i].replace('.', '')

            # Single letter should match first letter of full name
            if len(short_clean) == 1:
                if short_clean != long_clean[0]:
                    return False
            # Full names should match exactly
            else:
                if short_clean != long_clean:
                    return False

        return True

    def _nickname_match(self, name1: str, name2: str) -> bool:
        """
        Check if two names match through nickname mapping
        Handles: "Bob Smith" <-> "Robert Smith"
        """
        parts1 = name1.split()
        parts2 = name2.split()

        if len(parts1) != len(parts2):
            return False

        for p1, p2 in zip(parts1, parts2):
            p1_clean = p1.replace('.', '').lower()
            p2_clean = p2.replace('.', '').lower()

            # Exact match
            if p1_clean == p2_clean:
                continue

            # Check nickname variants
            p1_variants = self._get_name_variants(p1_clean)
            p2_variants = self._get_name_variants(p2_clean)

            # Check if any variant matches
            if not any(v1 in p2_variants or v2 in p1_variants
                      for v1 in p1_variants for v2 in p2_variants):
                return False

        return True

    def _enhanced_prefix_matching(self, name1: str, name2: str) -> float:
        """
        Calculate prefix matching score with sophisticated logic
        Returns confidence score 0-100
        """
        parts1 = name1.split()
        parts2 = name2.split()

        if len(parts1) != len(parts2) or len(parts1) == 0:
            return 0.0

        total_score = 0.0
        for i, (p1, p2) in enumerate(zip(parts1, parts2)):
            p1_clean = p1.replace('.', '').lower()
            p2_clean = p2.replace('.', '').lower()

            # Exact match gets full score
            if p1_clean == p2_clean:
                total_score += 100
            else:
                # Prefix matching logic
                shorter = min(p1_clean, p2_clean, key=len)
                longer = max(p1_clean, p2_clean, key=len)

                if len(shorter) >= 3 and longer.startswith(shorter):
                    # Prefix match score based on length ratio
                    ratio = len(shorter) / len(longer)
                    if i == len(parts1) - 1:  # Last name - stricter
                        total_score += 90 * ratio if ratio > 0.7 else 0
                    else:  # First/middle name - more lenient
                        total_score += 85 * ratio if ratio > 0.5 else 0

        return total_score / len(parts1)

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