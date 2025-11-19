import io
import csv
import pandas as pd 
import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)

class CSVAnalyzer:
    """
    Analyzes CSV files to detect header rows and data quality
    """

    def __init__(self):
        self.header_keywords = [
            'id', 'name', 'date', 'time', 'patient', 'study', 'bill', 
            'account', 'number', 'code', 'type', 'status', 'value',
            'amount', 'total', 'count', 'age', 'gender', 'address'
        ]
    
    def analyze_file(self, file_content: bytes, preview_lines: int = 20) -> Dict:
        """
        Analyze CSV file and detect structure
        
        Returns:
            {
                "preview": [...],  # First N lines as strings
                "detected_header_row": int,
                "confidence": float,
                "detected_skip_rows": List[int],
                "reasoning": str
            }
        """

        try:
            content_str = file_content.decode('utf-8', errors='ignore')
            lines = content_str.split('\n')[:preview_lines]
            
            header_row, confidence, reasoning = self._detect_header_row(lines)
            
            skip_rows = list(range(header_row)) if header_row > 0 else []
            
            return {
                "success": True,
                "preview": lines,
                "total_preview_lines": len(lines),
                "detected_header_row": header_row,
                "confidence": confidence,
                "detected_skip_rows": skip_rows,
                "reasoning": reasoning
            }
            
        except Exception as e:
            logger.error(f"CSV analysis error: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def _detect_header_row(self, lines:List[str]) -> Tuple[int, float, str]:
        """
        Use heuristics to detect which row is the header
        
        Returns:
            (row_index, confidence_score, reasoning_text)
        """
        scores = []
        
        for i, line in enumerate(lines):
            if not line.strip():
                continue  
            
            try:
                reader = csv.reader([line])
                row = next(reader)
                
                if len(row) < 2:
                    continue
                
                score = 0
                reasons = []
                
                # Heuristic 1: Contains header keywords
                keyword_matches = sum(
                    1 for col in row 
                    if any(keyword in col.lower() for keyword in self.header_keywords)
                )
                if keyword_matches > 0:
                    score += keyword_matches * 2
                    reasons.append(f"{keyword_matches} header keywords found")
                
                # Heuristic 2: No purely numeric columns (headers are text)
                non_numeric = sum(
                    1 for col in row 
                    if not col.strip().replace(',', '').replace('.', '').replace('-', '').isdigit()
                )
                if non_numeric == len(row):
                    score += 3
                    reasons.append("All columns are text (not numbers)")
                
                # Heuristic 3: All columns have content (no empties)
                if all(col.strip() for col in row):
                    score += 2
                    reasons.append("No empty columns")
                
                # Heuristic 4: Contains underscores/snake_case (common in headers)
                underscore_count = sum(1 for col in row if '_' in col)
                if underscore_count > 0:
                    score += underscore_count
                    reasons.append(f"{underscore_count} columns with underscores")
                
                # Heuristic 5: Column count consistency with next rows
                try:
                    next_rows = lines[i+1:i+5]
                    parsed_next = [list(csv.reader([r])) for r in next_rows if r.strip()]
                    
                    if parsed_next:
                        consistent = sum(
                            1 for parsed in parsed_next 
                            if len(parsed[0]) == len(row)
                        )
                        if consistent >= len(parsed_next) * 0.8:  # 80% consistency
                            score += 3
                            reasons.append(f"Consistent column count with next {consistent} rows")
                except:
                    pass
                
                # Heuristic 6: Penalize early rows (often metadata)
                if i == 0:
                    score -= 1  # Slight penalty for first row
                
                # Calculate confidence (0-100%)
                max_possible_score = 15  # Adjust based on heuristics
                confidence = min(100, (score / max_possible_score) * 100)
                
                scores.append({
                    "row": i,
                    "score": score,
                    "confidence": confidence,
                    "reasons": reasons,
                    "column_count": len(row)
                })
                
            except Exception as e:
                logger.debug(f"Error analyzing row {i}: {e}")
                continue
        
        if not scores:
            return 0, 50.0, "No valid rows found, defaulting to row 0"
        

        scores.sort(key=lambda x: x['score'], reverse=True)
        best = scores[0]
        
        reasoning = f"Row {best['row']}: " + "; ".join(best['reasons'])
        
        return best['row'], best['confidence'], reasoning



csv_analyzer = CSVAnalyzer()