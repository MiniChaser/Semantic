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
        Enhanced multi-tier author matching algorithm with position-aware disambiguation

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

        # Create position-aware author lists instead of dictionaries to prevent loss
        dblp_data = []
        for i, name in enumerate(dblp_authors):
            normalized = self.normalize_name(name)
            dblp_data.append({
                'original': name,
                'normalized': normalized,
                'position': i,
                'matched': False
            })

        s2_data = []
        for i, author in enumerate(valid_s2_authors):
            normalized = self.normalize_name(author['name'])
            s2_data.append({
                'original': author,
                'normalized': normalized,
                'position': i,
                'matched': False
            })

        # Initialize tracking
        matched = {}

        # Tier 1: Exact full name matching
        for dblp_item in dblp_data:
            if dblp_item['matched']:
                continue
            for s2_item in s2_data:
                if s2_item['matched']:
                    continue
                if dblp_item['normalized'] == s2_item['normalized']:
                    matched[dblp_item['original']] = s2_item['original']
                    dblp_item['matched'] = True
                    s2_item['matched'] = True
                    self.match_stats['exact_matches'] += 1
                    break

        # Tier 2: Position-aware disambiguation for duplicate normalized names
        # This is the key fix for the "Zhiyuan Liu 0010" vs "Zhiyuan Liu 0001" issue
        duplicates_map = {}
        for dblp_item in dblp_data:
            if dblp_item['matched']:
                continue
            norm = dblp_item['normalized']
            if norm not in duplicates_map:
                duplicates_map[norm] = {'dblp': [], 's2': []}
            duplicates_map[norm]['dblp'].append(dblp_item)

        for s2_item in s2_data:
            if s2_item['matched']:
                continue
            norm = s2_item['normalized']
            if norm in duplicates_map:
                duplicates_map[norm]['s2'].append(s2_item)

        # Handle duplicates with position-based matching
        for norm, items in duplicates_map.items():
            dblp_items = items['dblp']
            s2_items = items['s2']

            if len(dblp_items) > 1 and len(s2_items) > 1:
                # Multiple authors with same normalized name - use position matching
                for dblp_item in dblp_items:
                    if dblp_item['matched']:
                        continue
                    # Find S2 author with closest position
                    best_match = None
                    best_distance = float('inf')

                    for s2_item in s2_items:
                        if s2_item['matched']:
                            continue
                        distance = abs(dblp_item['position'] - s2_item['position'])
                        if distance < best_distance:
                            best_distance = distance
                            best_match = s2_item

                    if best_match:
                        matched[dblp_item['original']] = best_match['original']
                        dblp_item['matched'] = True
                        best_match['matched'] = True
                        self.match_stats['positional_matches'] += 1

        # Tier 3: Enhanced initialism matching for remaining authors
        for dblp_item in dblp_data:
            if dblp_item['matched']:
                continue
            dblp_parts = dblp_item['normalized'].split()
            if len(dblp_parts) >= 2:
                # Create initialism patterns
                dblp_patterns = [
                    ''.join(part[0] for part in dblp_parts[:-1]) + ' ' + dblp_parts[-1],
                    '.'.join(part[0] for part in dblp_parts[:-1]) + '. ' + dblp_parts[-1]
                ]

                for s2_item in s2_data:
                    if s2_item['matched']:
                        continue
                    s2_parts = s2_item['normalized'].split()
                    if len(s2_parts) >= 2:
                        s2_patterns = [
                            ''.join(part[0] for part in s2_parts[:-1]) + ' ' + s2_parts[-1],
                            '.'.join(part[0] for part in s2_parts[:-1]) + '. ' + s2_parts[-1]
                        ]

                        # Check pattern matches
                        for dblp_pattern in dblp_patterns:
                            for s2_pattern in s2_patterns:
                                if dblp_pattern == s2_pattern:
                                    matched[dblp_item['original']] = s2_item['original']
                                    dblp_item['matched'] = True
                                    s2_item['matched'] = True
                                    self.match_stats['initialism_matches'] += 1
                                    break
                            if dblp_item['matched']:
                                break
                        if dblp_item['matched']:
                            break
        # Tier 4: Fuzzy string matching for remaining authors
        for dblp_item in dblp_data:
            if dblp_item['matched']:
                continue

            best_match = None
            best_score = 0

            for s2_item in s2_data:
                if s2_item['matched']:
                    continue

                # Use multiple fuzzy matching algorithms
                token_sort_score = fuzz.token_sort_ratio(dblp_item['normalized'], s2_item['normalized'])
                token_set_score = fuzz.token_set_ratio(dblp_item['normalized'], s2_item['normalized'])
                partial_score = fuzz.partial_ratio(dblp_item['normalized'], s2_item['normalized'])

                combined_score = max(token_sort_score, token_set_score, partial_score)

                if combined_score > 85 and combined_score > best_score:
                    best_score = combined_score
                    best_match = s2_item

            if best_match:
                matched[dblp_item['original']] = best_match['original']
                dblp_item['matched'] = True
                best_match['matched'] = True
                self.match_stats['fuzzy_matches'] += 1

        # Count unmatched
        unmatched_count = sum(1 for item in dblp_data if not item['matched'])
        self.match_stats['unmatched'] += unmatched_count

        # Return unmatched DBLP authors
        unmatched = [item['original'] for item in dblp_data if not item['matched']]

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