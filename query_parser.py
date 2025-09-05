"""
Query Parser Module
Handles natural language query parsing and SQL generation
"""

import re
from typing import Dict, List, Any, Optional


class QueryParser:
    """Handles natural language query parsing and SQL generation"""
    
    def __init__(self, database_analyzer=None):
        self.analyzer = database_analyzer
    
    def analyze_user_query_context(self, natural_query: str) -> Dict[str, Any]:
        """Analyze user query to provide better context and suggestions"""
        query_lower = natural_query.lower()
        
        analysis = {
            'query_type': 'unknown',
            'complexity': 'simple',
            'suggested_tables': [],
            'suggested_columns': [],
            'query_intent': [],
            'data_requirements': []
        }
        
        # Determine query type
        if any(word in query_lower for word in ['count', 'how many', 'total number']):
            analysis['query_type'] = 'aggregation'
            analysis['query_intent'].append('count_records')
        elif any(word in query_lower for word in ['average', 'mean', 'avg']):
            analysis['query_type'] = 'aggregation'
            analysis['query_intent'].append('calculate_average')
        elif any(word in query_lower for word in ['sum', 'total']):
            analysis['query_type'] = 'aggregation'
            analysis['query_intent'].append('sum_values')
        elif any(word in query_lower for word in ['show', 'list', 'display', 'get']):
            analysis['query_type'] = 'retrieval'
            analysis['query_intent'].append('retrieve_data')
        elif any(word in query_lower for word in ['find', 'search', 'where']):
            analysis['query_type'] = 'filtered_retrieval'
            analysis['query_intent'].append('search_data')
        
        # Determine complexity
        complexity_indicators = len([word for word in ['where', 'and', 'or', 'join', 'group', 'order', 'having'] 
                                   if word in query_lower])
        if complexity_indicators >= 3:
            analysis['complexity'] = 'complex'
        elif complexity_indicators >= 1:
            analysis['complexity'] = 'moderate'
        
        # Suggest relevant tables based on keywords
        if self.analyzer:
            keywords = query_lower.split()
            for keyword in keywords:
                # Find tables with similar names
                matching_tables = [table_key for table_key in self.analyzer.filtered_tables.keys()
                                 if keyword in table_key.lower().split('.')[-1]]
                analysis['suggested_tables'].extend(matching_tables)
            
            # Remove duplicates and limit suggestions
            analysis['suggested_tables'] = list(set(analysis['suggested_tables']))[:5]
        
        return analysis
    
    def find_table_by_name(self, table_hint: str) -> Optional[str]:
        """Find table name from natural language hint"""
        if not self.analyzer:
            return None
            
        table_hint = table_hint.lower().strip()
        
        # Direct match
        if table_hint in self.analyzer.schema_info:
            return self.analyzer.schema_info[table_hint]
        
        # Partial match
        for table_name, full_name in self.analyzer.schema_info.items():
            if table_hint in table_name or table_name in table_hint:
                return full_name
        
        # Keywords matching
        for table_name, full_name in self.analyzer.schema_info.items():
            if any(word in table_name for word in table_hint.split()):
                return full_name
        
        return None
    
    def find_columns_by_hint(self, table_name: str, column_hints: List[str]) -> List[str]:
        """Find column names from natural language hints"""
        if not self.analyzer or table_name not in self.analyzer.table_columns:
            return []
        
        columns = self.analyzer.table_columns[table_name]
        matched_columns = []
        
        for hint in column_hints:
            hint = hint.lower().strip()
            
            # Direct match
            for col in columns:
                if hint == col['name'].lower():
                    matched_columns.append(col['name'])
                    break
            else:
                # Partial match
                for col in columns:
                    if hint in col['name'].lower() or col['name'].lower() in hint:
                        matched_columns.append(col['name'])
                        break
        
        return matched_columns
    
    def parse_natural_language(self, query: str) -> Dict[str, Any]:
        """Parse natural language query into structured components with enhanced analysis"""
        query = query.lower().strip()
        
        # Get query context analysis
        context = self.analyze_user_query_context(query)
        
        parsed = {
            'action': 'SELECT',
            'table': None,
            'columns': [],
            'conditions': [],
            'limit': None,
            'order_by': [],
            'group_by': [],
            'aggregations': [],
            'context': context
        }
        
        # Use context to improve parsing
        if context['query_type'] == 'aggregation':
            if 'count_records' in context['query_intent']:
                parsed['aggregations'].append('COUNT')
            elif 'calculate_average' in context['query_intent']:
                parsed['aggregations'].append('AVG')
            elif 'sum_values' in context['query_intent']:
                parsed['aggregations'].append('SUM')
        
        # Enhanced table detection using context suggestions
        parsed['table'] = self._extract_table_from_query(query, context)
        
        # Extract column hints
        parsed['columns'] = self._extract_columns_from_query(query, parsed['table'])
        
        # Extract conditions (WHERE clauses)
        parsed['conditions'] = self._extract_conditions_from_query(query, parsed['table'])
        
        # Extract LIMIT
        parsed['limit'] = self._extract_limit_from_query(query)
        
        # Extract ORDER BY
        parsed['order_by'] = self._extract_order_by_from_query(query, parsed['table'])
        
        return parsed
    
    def _extract_table_from_query(self, query: str, context: Dict) -> Optional[str]:
        """Extract table name from query"""
        table_patterns = [
            r'from\s+(\w+)',
            r'in\s+(\w+)\s+table',
            r'(\w+)\s+table',
            r'all\s+(\w+)',
        ]
        
        for pattern in table_patterns:
            match = re.search(pattern, query)
            if match:
                table_hint = match.group(1)
                found_table = self.find_table_by_name(table_hint)
                if found_table:
                    return found_table
        
        # If no table found, use context suggestions
        if context.get('suggested_tables'):
            return context['suggested_tables'][0]
        
        # If still no table found, try to extract from context
        words = query.split()
        for word in words:
            table_match = self.find_table_by_name(word)
            if table_match:
                return table_match
        
        return None
    
    def _extract_columns_from_query(self, query: str, table_name: str) -> List[str]:
        """Extract column names from query"""
        column_patterns = [
            r'show\s+([\w\s,]+?)(?:\s+from|\s+where|$)',
            r'get\s+([\w\s,]+?)(?:\s+from|\s+where|$)',
            r'select\s+([\w\s,]+?)(?:\s+from|\s+where|$)',
        ]
        
        for pattern in column_patterns:
            match = re.search(pattern, query)
            if match:
                column_text = match.group(1).strip()
                if column_text not in ['all', '*', 'everything']:
                    column_hints = [col.strip() for col in re.split(r'[,\s]+', column_text) if col.strip()]
                    if table_name:
                        return self.find_columns_by_hint(table_name, column_hints)
                break
        
        return []
    
    def _extract_conditions_from_query(self, query: str, table_name: str) -> List[str]:
        """Extract WHERE conditions from query"""
        condition_patterns = [
            r'where\s+(.+?)(?:\s+order|\s+group|\s+limit|$)',
            r'with\s+(.+?)(?:\s+order|\s+group|\s+limit|$)',
            r'that\s+(.+?)(?:\s+order|\s+group|\s+limit|$)',
        ]
        
        for pattern in condition_patterns:
            match = re.search(pattern, query)
            if match:
                condition_text = match.group(1).strip()
                return self.parse_conditions(condition_text, table_name)
        
        return []
    
    def _extract_limit_from_query(self, query: str) -> Optional[int]:
        """Extract LIMIT from query"""
        limit_patterns = [
            r'(?:top|first|limit)\s+(\d+)',
            r'(\d+)\s+(?:rows?|records?)',
        ]
        
        for pattern in limit_patterns:
            match = re.search(pattern, query)
            if match:
                return int(match.group(1))
        
        return None
    
    def _extract_order_by_from_query(self, query: str, table_name: str) -> List[tuple]:
        """Extract ORDER BY from query"""
        if not any(phrase in query for phrase in ['order by', 'sort by', 'ordered by']):
            return []
        
        order_patterns = [
            r'order\s+by\s+([\w\s,]+?)(?:\s+limit|$)',
            r'sort\s+by\s+([\w\s,]+?)(?:\s+limit|$)',
            r'ordered\s+by\s+([\w\s,]+?)(?:\s+limit|$)',
        ]
        
        for pattern in order_patterns:
            match = re.search(pattern, query)
            if match:
                order_text = match.group(1).strip()
                if table_name:
                    order_columns = self.find_columns_by_hint(table_name, [order_text])
                    if order_columns:
                        direction = 'DESC' if 'desc' in query or 'descending' in query else 'ASC'
                        return [(order_columns[0], direction)]
                break
        
        return []
    
    def parse_conditions(self, condition_text: str, table_name: str) -> List[str]:
        """Parse condition text into SQL WHERE clauses"""
        if not self.analyzer or not table_name or table_name not in self.analyzer.table_columns:
            return []
        
        conditions = []
        
        # Simple condition patterns
        patterns = [
            (r'(\w+)\s*=\s*["\']([^"\']+)["\']', lambda m: f"{m.group(1)} = '{m.group(2)}'"),
            (r'(\w+)\s*=\s*(\d+)', lambda m: f"{m.group(1)} = {m.group(2)}"),
            (r'(\w+)\s*>\s*(\d+)', lambda m: f"{m.group(1)} > {m.group(2)}"),
            (r'(\w+)\s*<\s*(\d+)', lambda m: f"{m.group(1)} < {m.group(2)}"),
            (r'(\w+)\s*like\s*["\']([^"\']+)["\']', lambda m: f"{m.group(1)} LIKE '%{m.group(2)}%'"),
            (r'(\w+)\s*contains\s*["\']([^"\']+)["\']', lambda m: f"{m.group(1)} LIKE '%{m.group(2)}%'"),
        ]
        
        for pattern, formatter in patterns:
            matches = re.finditer(pattern, condition_text, re.IGNORECASE)
            for match in matches:
                column_hint = match.group(1)
                matched_columns = self.find_columns_by_hint(table_name, [column_hint])
                if matched_columns:
                    condition = formatter(match).replace(column_hint, matched_columns[0])
                    conditions.append(condition)
        
        return conditions
    
    def build_sql_query(self, parsed: Dict[str, Any]) -> str:
        """Build SQL query from parsed natural language"""
        if not parsed['table']:
            return "-- Error: No table identified in the query"
        
        # SELECT clause
        if parsed['aggregations']:
            if 'COUNT' in parsed['aggregations']:
                select_clause = "SELECT COUNT(*)"
            elif parsed['columns']:
                agg = parsed['aggregations'][0]
                col = parsed['columns'][0] if parsed['columns'] else '*'
                select_clause = f"SELECT {agg}({col})"
            else:
                select_clause = f"SELECT {parsed['aggregations'][0]}(*)"
        elif parsed['columns']:
            select_clause = f"SELECT {', '.join(parsed['columns'])}"
        else:
            select_clause = "SELECT *"
        
        # Add TOP clause if limit specified
        if parsed['limit'] and 'COUNT' not in parsed.get('aggregations', []):
            select_clause = select_clause.replace('SELECT', f'SELECT TOP {parsed["limit"]}')
        
        # FROM clause
        from_clause = f"FROM {parsed['table']}"
        
        # WHERE clause
        where_clause = ""
        if parsed['conditions']:
            where_clause = f"WHERE {' AND '.join(parsed['conditions'])}"
        
        # ORDER BY clause
        order_clause = ""
        if parsed['order_by']:
            order_items = [f"{col} {direction}" for col, direction in parsed['order_by']]
            order_clause = f"ORDER BY {', '.join(order_items)}"
        
        # Combine all clauses
        sql_parts = [select_clause, from_clause]
        if where_clause:
            sql_parts.append(where_clause)
        if order_clause:
            sql_parts.append(order_clause)
        
        return '\n'.join(sql_parts)
    
    def natural_query_to_sql(self, natural_query: str) -> tuple:
        """Complete pipeline: natural language -> SQL with context"""
        print(f"🤖 Processing: '{natural_query}'")
        
        # Parse natural language with enhanced context
        parsed = self.parse_natural_language(natural_query)
        context = parsed.get('context', {})
        
        print(f"📝 Query Analysis:")
        print(f"   Type: {context.get('query_type', 'unknown')}")
        print(f"   Complexity: {context.get('complexity', 'simple')}")
        print(f"   Intent: {', '.join(context.get('query_intent', []))}")
        print(f"   Table: {parsed['table']}")
        print(f"   Columns: {parsed['columns']}")
        print(f"   Conditions: {parsed['conditions']}")
        
        # Build SQL
        sql = self.build_sql_query(parsed)
        
        return sql, context