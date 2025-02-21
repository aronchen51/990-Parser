import xml.etree.ElementTree as ET
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from collections import defaultdict
import logging
from bs4 import BeautifulSoup
import re
import os


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProPublicaScraper:
    """Scraper for ProPublica nonprofit search pages"""
    
    def __init__(self):
        self.base_url = "https://projects.propublica.org"
    
    def get_organization_links(self, main_url):
        """
        Extract XML download links and NTEE category from organization's main page
        Returns: tuple (ntee_category, list_of_xml_urls)
        """
        try:
            # Fetch the main page
            response = requests.get(main_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract NTEE category
            ntee_elem = soup.find('p', class_='ntee-category')
            if ntee_elem:
                ntee_category = ntee_elem.text.split(':', 1)[1].strip().split('/')[0].strip()
            else:
                ntee_category = "Unknown"
            
            # Find all XML download links
            xml_links = []
            for link in soup.find_all('a', class_='btn', target='_blank'):
                if 'XML' in link.text:
                    object_id = link['href'].split('object_id=')[1]
                    full_url = f"{self.base_url}/nonprofits/download-xml?object_id={object_id}"
                    xml_links.append(full_url)
            
            # Sort links by object_id (which contains year) and take most recent 5
            xml_links.sort(reverse=True)
            xml_links = xml_links[:5]
            
            return ntee_category, xml_links
            
        except requests.RequestException as e:
            logger.error(f"Error fetching organization page: {str(e)}")
            raise


class NonprofitParser:
    """Parser for nonprofit financial data from ProPublica URLs"""
    
    def __init__(self):
        # Initialize scraper and remove test_urls since we'll use dynamic scraping
        self.scraper = ProPublicaScraper()
        self.ns = {'irs': 'http://www.irs.gov/efile'}
        self.leadership_titles = [
            'PRESIDENT', 'CEO', 'CHIEF EXECUTIVE OFFICER',
            'CFO', 'CHIEF FINANCIAL OFFICER',
            'COO', 'CHIEF OPERATING OFFICER',
            'CHANCELLOR', 'DEAN',
            'TREASURER'
        ]

    def fetch_content(self, url):
        """Fetch content from URL with error handling"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.content
        except requests.RequestException as e:
            logger.error(f"Failed to fetch from URL {url}: {str(e)}")
            raise

    def detect_format(self, content):
        """
        Detect whether content is XML or TXT format
        Returns tuple of (format_type, parsed_content)
        """
        try:
            # First try parsing as XML
            tree = ET.parse(BytesIO(content))
            logger.info("Successfully parsed as XML")
            return 'xml', tree
        except ET.ParseError:
            # If XML parsing fails, try TXT format
            try:
                text_content = content.decode('utf-8', errors='ignore')
                # Check for common TXT format markers
                if any(marker in text_content.upper() for marker in 
                      ['RETURN HEADER', 'FORM 990', 'EIN:']):
                    logger.info("Successfully parsed as TXT")
                    return 'txt', text_content
                else:
                    raise ValueError("Content doesn't match expected formats")
            except Exception as e:
                logger.error(f"Format detection failed: {str(e)}")
                raise

    def get_tax_year(self, content, format_type):
        """Extract tax year from content and adjust it to reflect reporting year"""
        try:
            if format_type == 'xml':
                root = content.getroot()
                # Try multiple possible locations for tax year
                tax_period = root.find('.//irs:TaxPeriodEndDt', self.ns)
                if tax_period is not None and tax_period.text:
                    # Subtract 1 from the tax year to get the reporting year
                    return str(int(datetime.strptime(tax_period.text, '%Y-%m-%d').year) - 1)
                
                tax_year = root.find('.//irs:TaxYr', self.ns)
                if tax_year is not None and tax_year.text:
                    # Subtract 1 from the tax year to get the reporting year
                    return str(int(tax_year.text) - 1)
            else:
                # Search for year in TXT content
                text_lines = content.split('\n')
                for line in text_lines:
                    if 'Tax Period Begin' in line:
                        # Extract first 4-digit number found and subtract 1
                        for word in line.split():
                            if word.isdigit() and len(word) == 4:
                                return str(int(word) - 1)
            
            logger.warning("Could not find tax year")
            return "Unknown"
        except Exception as e:
            logger.error(f"Error extracting tax year: {str(e)}")
            return "Unknown"

    def get_organization_name(self, content, format_type):
        """Extract organization name from content"""
        try:
            if format_type == 'xml':
                root = content.getroot()
                # Try multiple possible locations for organization name
                for path in [
                    './/irs:BusinessName/irs:BusinessNameLine1Txt',
                    './/irs:ReturnHeader/irs:Filer/irs:BusinessName/irs:BusinessNameLine1Txt'
                ]:
                    name = root.find(path, self.ns)
                    if name is not None and name.text:
                        return name.text
            else:
                # Search for organization name in TXT content
                text_lines = content.split('\n')
                for line in text_lines:
                    if 'Name of Organization:' in line or 'NAME OF ORGANIZATION:' in line:
                        return line.split(':', 1)[1].strip()
            
            logger.warning("Could not find organization name")
            return "Unknown Organization"
        except Exception as e:
            logger.error(f"Error extracting organization name: {str(e)}")
            return "Unknown Organization"

    def process_url(self, url):
        """Process a single URL and return basic information"""
        try:
            content = self.fetch_content(url)
            format_type, parsed_content = self.detect_format(content)
            
            return {
                'url': url,
                'format': format_type,
                'tax_year': self.get_tax_year(parsed_content, format_type),
                'organization_name': self.get_organization_name(parsed_content, format_type),
                'parsed_content': parsed_content
            }
        except Exception as e:
            logger.error(f"Error processing URL {url}: {str(e)}")
            raise

class FinancialDataExtractor:
    """Extracts financial data from parsed nonprofit documents"""
    
    def __init__(self):
            self.ns = {'irs': 'http://www.irs.gov/efile'}
            # Add leadership titles here
            self.leadership_titles = [
                'PRESIDENT', 'CEO', 'CHIEF EXECUTIVE',
                'CFO', 'CHIEF FINANCIAL',
                'COO', 'CHIEF OPERATING',
                'CHANCELLOR', 'DEAN','TREASURER'
                # TODO: Add more leadership titles as needed
            ]

    
    def extract_financial_metrics(self, content, format_type):
        """Extract basic financial metrics"""
        try:
            if format_type == 'xml':
                return self._extract_financial_metrics_xml(content)
            else:
                return self._extract_financial_metrics_txt(content)
        except Exception as e:
            logger.error(f"Error extracting financial metrics: {str(e)}")
            return {}

    def _extract_financial_metrics_xml(self, tree):
        root = tree.getroot()
        metrics = {}

        # Handle TotalFunctionalExpensesGrp
        total_expenses = root.find('.//irs:TotalFunctionalExpensesGrp', self.ns)
        if total_expenses is not None:
            mgmt_total = total_expenses.find('.//irs:ManagementAndGeneralAmt', self.ns)
            fundraising_total = total_expenses.find('.//irs:FundraisingAmt', self.ns)
            if mgmt_total is not None:
                metrics['ManagementAndGeneralAmt'] = mgmt_total.text
            if fundraising_total is not None:
                metrics['CYTotalFundraisingExpenseAmt'] = fundraising_total.text

        # Handle group elements
        group_elements = {
            'InformationTechnologyGrp': 'InformationTechnologyGrp',
            'OccupancyGrp': 'OccupancyGrp',
            'TravelGrp': 'TravelGrp'
        }

        for field, xml_tag in group_elements.items():
            group = root.find(f'.//irs:{xml_tag}', self.ns)
            if group is not None:
                total = group.find('.//irs:TotalAmt', self.ns)
                if total is not None:
                    metrics[field] = total.text
        
        donor_restriction_paths = {
            'WithoutDonorRestrictions': [
                ('.//irs:NoDonorRestrictionNetAssetsGrp/irs:EOYAmt', self.ns),
                ('.//irs:UnrestrictedNetAssetsGrp/irs:EOYAmt', self.ns)
            ],
            'WithDonorRestrictions': [
                ('.//irs:DonorRestrictionNetAssetsGrp/irs:EOYAmt', self.ns),
                ('.//irs:PermanentlyRstrNetAssetsGrp/irs:EOYAmt', self.ns)
            ]
        }
        
        for metric, paths in donor_restriction_paths.items():
            for path, ns in paths:
                value = root.find(path, ns)
                if value is not None and value.text:
                    metrics[metric] = value.text
                    break
            if metric not in metrics:
                metrics[metric] = 'Not found'
        
    
    
        balance_sheet_groups = {
            'CashNonInterestBearing': 'CashNonInterestBearingGrp',
            'AccountsReceivable': 'AccountsReceivableGrp',
            'AccountsPayable': 'AccountsPayableAccrExpnssGrp'
        }

        # Process each balance sheet group
        for field, group_name in balance_sheet_groups.items():
            group = root.find(f'.//irs:{group_name}', self.ns)
            if group is not None:
                eoy_amt = group.find('.//irs:EOYAmt', self.ns)
                if eoy_amt is not None:
                    metrics[f'{field}EOY'] = eoy_amt.text



        # Basic financial elements to extract
        financial_elements = {
            'revenue': [
                'CYTotalRevenueAmt',
                'CYContributionsGrantsAmt',
                'CYProgramServiceRevenueAmt',
                'InvestmentIncomeAmt',
                'CYOtherRevenueAmt',
                'CYInvestmentIncomeAmt',
                'CYRevenuesLessExpensesAmt'
            ],
            'expenses': [
                'CYTotalExpensesAmt',
                'CYGrantsAndSimilarPaidAmt',
                'CYSalariesCompEmpBnftPaidAmt',
                'TotalProgramServiceExpensesAmt',
                'FundraisingAmt',
                'CYOtherExpensesAmt',
                'OtherEmployeeBenefitsGrp/TotalAmt'
            ],
            'assets': [
                'TotalAssetsEOYAmt',
                'TotalLiabilitiesEOYAmt',
                'NetAssetsOrFundBalancesEOYAmt'
            ],

            'balance_sheet': [
                'CashNonInterestBearingGrp/EOYAmt',
                'AccountsReceivableGrp/EOYAmt',
                'AccountsPayableAccrExpnssGrp/EOYAmt'
            ],
            'other': [
                'TotalEmployeeCnt',
                'TotalVolunteersCnt'               
            ]
        }
        
        # Process regular financial elements
        for category, elements in financial_elements.items():
            for element in elements:
                paths = [
                    f'.//irs:{element}',
                    f'.//irs:IRS990/{element}',
                    f'.//irs:Form990PartIX/{element}'
                ]
                
                value = None
                for path in paths:
                    value = root.find(path, self.ns)
                    if value is not None:
                        break
                
                metrics[element] = value.text if value is not None else 'Not found'
                
        return metrics

    def _extract_financial_metrics_txt(self, content):
        metrics = {}
        lines = content.split('\n')

        # Add balance sheet patterns
        balance_sheet_patterns = {
            'CashNonInterestBearingEOY': ['CASH NON-INTEREST BEARING', 'CASH - NON-INTEREST BEARING'],
            'AccountsReceivableEOY': ['ACCOUNTS RECEIVABLE'],
            'AccountsPayableEOY': ['ACCOUNTS PAYABLE', 'ACCOUNTS PAYABLE AND ACCRUED EXPENSES']
        }

        # Process balance sheet items
        for field, patterns in balance_sheet_patterns.items():
            for pattern in patterns:
                for i, line in enumerate(lines):
                    if pattern in line.upper():
                        # Look for EOY amount in this line and next few lines
                        for j in range(i, min(i + 5, len(lines))):
                            line_text = lines[j].upper()
                            if 'END OF YEAR' in line_text or 'EOY' in line_text:
                                value = self._extract_numeric_value(lines[j])
                                if value:
                                    metrics[field] = value
                                    break
                        break
        
        # Find total functional expenses
        for i, line in enumerate(lines):
            if 'TOTAL FUNCTIONAL EXPENSES' in line.upper():
                # Look for management and fundraising amounts
                for j in range(i, min(i + 10, len(lines))):
                    if 'MANAGEMENT AND GENERAL' in lines[j].upper():
                        value = self._extract_numeric_value(lines[j])
                        if value:
                            metrics['ManagementAndGeneralAmt'] = value
                    if 'FUNDRAISING' in lines[j].upper():
                        value = self._extract_numeric_value(lines[j])
                        if value:
                            metrics['CYTotalFundraisingExpenseAmt'] = value
        group_patterns = {
            'InformationTechnologyGrp': ['Information Technology', 'IT Expenses'],
            'OccupancyGrp': ['Occupancy', 'Occupancy Expenses'],
            'TravelGrp': ['Travel', 'Travel Expenses']
        }
    
        for field, patterns in group_patterns.items():
            for pattern in patterns:
                for i, line in enumerate(lines):
                    if pattern.upper() in line.upper():
                        value = self._extract_numeric_value(line)
                        if value:
                            metrics[field] = value
                            break
        
    # Handle donor restrictions
        donor_restriction_patterns = {
            'WithoutDonorRestrictions': [
                'NO DONOR RESTRICTION', 'UNRESTRICTED NET ASSETS',
                'WITHOUT DONOR RESTRICTIONS'
            ],
            'WithDonorRestrictions': [
                'DONOR RESTRICTION', 'PERMANENTLY RESTRICTED',
                'WITH DONOR RESTRICTIONS'
            ]
        }

        for metric, patterns in donor_restriction_patterns.items():
            for pattern in patterns:
                for i, line in enumerate(lines):
                    if pattern in line.upper() and 'END OF YEAR' in line.upper():
                        value = self._extract_numeric_value(line)
                        if value:
                            metrics[metric] = value
                            break


        # Existing field patterns
        field_patterns = {
            'CYTotalRevenueAmt': ['Total revenue', 'TOTAL REVENUE'],
            'CYTotalExpensesAmt': ['Total expenses', 'TOTAL EXPENSES'],
            'TotalAssetsEOYAmt': ['Total assets', 'TOTAL ASSETS'],
            'TotalLiabilitiesEOYAmt': ['Total liabilities', 'TOTAL LIABILITIES'],
            'NetAssetsOrFundBalancesEOYAmt': ['Total net assets', 'NET ASSETS OR FUND BALANCES'],
            'TotalProgramServiceExpensesAmt': ['Total program service expenses', 'PROGRAM SERVICE EXPENSES'],
            'FundraisingExpensesAmt': ['Fundraising expenses', 'FUNDRAISING EXPENSES'],
            'OtherEmployeeBenefitsAmt': ['Other employee benefits', 'EMPLOYEE BENEFITS'],
            'CYRevenuesLessExpensesAmt': ['Revenue less expenses', 'REVENUE LESS EXPENSES'],
            'CYInvestmentIncomeAmt': ['Investment income', 'INVESTMENT INCOME'],
            'TotalEmployeeCnt': ['Total number of employees', 'NUMBER OF EMPLOYEES'],
            'TotalVolunteersCnt': ['Total number of volunteers', 'NUMBER OF VOLUNTEERS']
        }
        
        for field, patterns in field_patterns.items():
            for pattern in patterns:
                for i, line in enumerate(lines):
                    if pattern.upper() in line.upper():
                        for j in range(i, min(i + 3, len(lines))):
                            value = self._extract_numeric_value(lines[j])
                            if value:
                                metrics[field] = value
                                break
                        
                        if field not in metrics:
                            metrics[field] = 'Not found'
        
        return metrics

    def extract_executive_compensation(self, content, format_type):
        """Extract executive compensation data"""
        try:
            if format_type == 'xml':
                return self._extract_executive_compensation_xml(content)
            else:
                return self._extract_executive_compensation_txt(content)
        except Exception as e:
            logger.error(f"Error extracting executive compensation: {str(e)}")
            return []

    def _extract_executive_compensation_xml(self, tree):
        """Extract executive compensation from XML format"""
        root = tree.getroot()
        executives = []
        
        # Look for compensation data in Form 990 Part VII
        for person in root.findall('.//irs:Form990PartVIISectionAGrp', self.ns):
            name = person.find('.//irs:PersonNm', self.ns)
            title = person.find('.//irs:TitleTxt', self.ns)
            compensation = person.find('.//irs:ReportableCompFromOrgAmt', self.ns)
            
            if all(elem is not None for elem in [name, title, compensation]):
                if self._is_leadership_title(title.text):
                    executives.append({
                        'name': name.text,
                        'title': title.text,
                        'compensation': compensation.text
                    })
        
        return executives
        
    def _extract_executive_compensation_txt(self, content):
        """Extract executive compensation from TXT format"""
        executives = []
        lines = content.split('\n')
            
        current_person = {}
        for i, line in enumerate(lines):
            line = line.upper()
            # Look for sections that typically contain compensation information
            if 'FORM 990, PART VII' in line or 'COMPENSATION OF OFFICERS' in line:
                # Look through next several lines for compensation information
                for j in range(i, min(i + 100, len(lines))):
                    line = lines[j].strip().upper()
                    
                    # Check for leadership titles
                    if any(title in line for title in self.leadership_titles):
                        # Try to extract name, title, and compensation
                        parts = line.split()
                        # Look for dollar amounts
                        for k, part in enumerate(parts):
                            if '$' in part or (part.replace(',', '').isdigit() and len(part) > 4):
                                try:
                                    compensation = part.replace('$', '').replace(',', '')
                                    # Assume title is before compensation and name is at start
                                    title = ' '.join(parts[1:k])  # Skip first word (assume it's part of name)
                                    name = parts[0]  # Just take first word as name for simplicity
                                    
                                    executives.append({
                                        'name': name,
                                        'title': title,
                                        'compensation': compensation
                                    })
                                    break
                                except ValueError:
                                    continue
            
        return executives

    def extract_endowment_data(self, content, format_type):
        """Extract endowment data from Schedule D Part V"""
        try:
            if format_type == 'xml':
                return self._extract_endowment_data_xml(content)
            else:
                return self._extract_endowment_data_txt(content)
        except Exception as e:
            logger.error(f"Error extracting endowment data: {str(e)}")
            return {}

    def _extract_endowment_data_xml(self, tree):
        """Extract endowment data from XML format"""
        root = tree.getroot()
        endowment_data = {}
        
        # Schedule D endowment fields mapping
        field_mapping = {
            'BeginningBalance': 'BeginningYearBalanceAmt',
            'Contributions': 'ContributionsAmt',
            'InvestmentEarnings': 'InvestmentEarningsOrLossesAmt',
            'Grants': 'GrantsOrScholarshipsAmt',
            'OtherExpenditures': 'OtherExpendituresAmt',
            'AdminExpenses': 'AdministrativeExpensesAmt',
            'EndingBalance': 'EndYearBalanceAmt'
        }
        
        # Year group mapping
        year_groups = {
            0: 'CYEndwmtFundGrp',
            1: 'CYMinus1YrEndwmtFundGrp',
            2: 'CYMinus2YrEndwmtFundGrp',
            3: 'CYMinus3YrEndwmtFundGrp',
            4: 'CYMinus4YrEndwmtFundGrp'
        }
        
        schedule_d = root.find('.//irs:IRS990ScheduleD', self.ns)
        if schedule_d is not None:
            for year_offset, group_name in year_groups.items():
                year_data = {}
                group = schedule_d.find(f'.//irs:{group_name}', self.ns)
                
                if group is not None:
                    for field, xml_tag in field_mapping.items():
                        value = group.find(f'.//irs:{xml_tag}', self.ns)
                        year_data[field] = value.text if value is not None else 'Not found'
                    
                    endowment_data[f'Year_{year_offset}'] = year_data
        
        return endowment_data
        
        '''for year_offset in range(5):  # Current year and previous 4 years
            year_data = {}
            for field, xml_tag in endowment_fields.items():
                # Adjust tag for previous years if necessary
                if year_offset > 0:
                    xml_tag = f"{xml_tag}Pyr{year_offset}"
                
                value = root.find(f'.//irs:{xml_tag}', self.ns)
                year_data[field] = value.text if value is not None else 'Not found'
            
            endowment_data[f'Year_{year_offset}'] = year_data
        
        return endowment_data'''

    def _extract_endowment_data_txt(self, content):
        """Extract endowment data from TXT format"""
        # Implementation for TXT format would go here
        # This is more complex due to varying formats and would need careful pattern matching
        return {}

    def _is_leadership_title(self, title):
        """Check if a title matches leadership positions"""
        if not title:
            return False
        
        return any(leadership_title in title.upper() for leadership_title in self.leadership_titles)

    def _extract_numeric_value(self, line):
        """Extract numeric value from text line"""
        try:
            # Remove common currency formatting
            clean_line = line.replace('$', '').replace(',', '').strip()
            # Find last sequence of digits (possibly with decimal point)
            words = clean_line.split()
            for word in reversed(words):
                try:
                    return str(float(word))
                except ValueError:
                    continue
            return None
        except Exception:
            return None



class ExcelOutputHandler:
    """Handles formatting and writing data to Excel in horizontal format with append capability"""

    def __init__(self, output_path):
        self.output_path = output_path
        self.field_mapping = {
            # Regular financial fields
            'Total Revenue': 'CYTotalRevenueAmt',
            'Total Contributions': 'CYContributionsGrantsAmt',
            'Grants and Salaries': 'CYGrantsAndSimilarPaidAmt',
            'Salaries Other': 'CYSalariesCompEmpBnftPaidAmt',
            'Total Expenses': 'CYTotalExpensesAmt',
            'Program Service Expenses': 'TotalProgramServiceExpensesAmt',
            'Management': 'ManagementAndGeneralAmt',
            'Fundraising': 'CYTotalFundraisingExpenseAmt',
            'Revenue Less': 'CYRevenuesLessExpensesAmt',
            'Information Technology': 'InformationTechnologyGrp',
            'Occupancy': 'OccupancyGrp',
            'Travel': 'TravelGrp',
            'Number of Employees': 'TotalEmployeeCnt',
            'Number of Volunteers': 'TotalVolunteersCnt',
            'Investment Income': 'CYInvestmentIncomeAmt',
            'Other Revenue': 'CYOtherRevenueAmt',
            'Program Service': 'CYProgramServiceRevenueAmt',
            'Other Expenses': 'CYOtherExpensesAmt',
            'Cash NonInterest Bearing': 'CashNonInterestBearingEOY',
            'Accounts Receivable': 'AccountsReceivableEOY',
            'Accounts Payable': 'AccountsPayableEOY',
            'Total Assets': 'TotalAssetsEOYAmt',
            'Total Liabilities': 'TotalLiabilitiesEOYAmt',
            'Net Assets': 'NetAssetsOrFundBalancesEOYAmt',
            'Net Assets Without Donor Restrictions': 'WithoutDonorRestrictions',
            'Net Assets With Donor Restrictions': 'WithDonorRestrictions',
            # Endowment fields
            'Endowment Beginning Balance': 'BeginningBalance',
            'Endowment Contributions': 'Contributions',
            'Endowment Investment Earnings': 'InvestmentEarnings',
            'Endowment Grants': 'Grants',
            'Endowment Other Expenditures': 'OtherExpenditures',
            'Endowment Admin Expenses': 'AdminExpenses',
            'Endowment Ending Balance': 'EndingBalance',
        }

    def clean_sheet_name(self, name):
        """Clean sheet name to comply with Excel restrictions"""
        invalid_chars = '[]:*?/\\'
        name = ''.join(c for c in name if c not in invalid_chars)
        return name[:31]
    
    def format_value(self, value):
        """Format numeric values appropriately"""
        if isinstance(value, str):
            try:
                clean_value = ''.join(c for c in value if c.isdigit() or c in '.-')
                return float(clean_value)
            except (ValueError, TypeError):
                return value
        return value

    def read_existing_data(self):
        """Read existing data from Excel file if it exists"""
        existing_data = {}
        try:
            if os.path.exists(self.output_path):
                existing_dfs = pd.read_excel(self.output_path, sheet_name=None)
                for sheet_name, df in existing_dfs.items():
                    existing_data[sheet_name] = df
        except Exception as e:
            logger.error(f"Error reading existing Excel file: {str(e)}")
        return existing_data

    def merge_data(self, existing_dfs, new_category_dfs):
        """Merge existing data with new data"""
        merged_dfs = {}
        
        # Process each category in new data
        for category, new_df in new_category_dfs.items():
            clean_category = self.clean_sheet_name(category)
            
            if clean_category in existing_dfs:
                # Read existing data
                existing_df = existing_dfs[clean_category]
                
                # Create composite key for identifying duplicates
                existing_df['_composite_key'] = existing_df['Organization'] + '_' + existing_df['Year'].astype(str)
                new_df['_composite_key'] = new_df['Organization'] + '_' + new_df['Year'].astype(str)
                
                # Remove rows from existing data that would be updated
                existing_df = existing_df[~existing_df['_composite_key'].isin(new_df['_composite_key'])]
                
                # Combine existing and new data
                merged_df = pd.concat([existing_df, new_df], ignore_index=True)
                merged_df = merged_df.drop('_composite_key', axis=1)
                
                # Sort by Organization and Year
                merged_df = merged_df.sort_values(['Organization', 'Year'])
                
                merged_dfs[clean_category] = merged_df
            else:
                merged_dfs[clean_category] = new_df
                
        # Include categories that only exist in the existing data
        for category, df in existing_dfs.items():
            if category not in merged_dfs:
                merged_dfs[category] = df
                
        return merged_dfs

    def consolidate_data(self, org_data):
        """Consolidate data into horizontal format, grouped by NTEE category"""
        category_dfs = {}
        
        for ntee_category, orgs in org_data.items():
            rows = []
            
            for org_name, years_data in orgs.items():
                # Sort years_data by tax_year
                years_data = sorted(years_data, key=lambda x: x['tax_year'])
                
                for year_data in years_data:
                    row = {
                        'Organization': org_name,
                        'Year': year_data['tax_year']
                    }
                    
                    # Add financial metrics
                    metrics = year_data.get('financial_metrics', {})
                    endowment_data = year_data.get('endowment_data', {}).get('Year_0', {})
                    
                    # Add metrics using field_mapping
                    for display_col, field_name in self.field_mapping.items():
                        if display_col.startswith('Endowment '):
                            value = endowment_data.get(field_name, '')
                        else:
                            value = metrics.get(field_name, '')
                        row[display_col] = self.format_value(value)
                    
                    rows.append(row)
            
            if rows:
                df = pd.DataFrame(rows)
                # Ensure all columns exist
                column_order = ['Organization', 'Year'] + list(self.field_mapping.keys())
                for col in column_order:
                    if col not in df.columns:
                        df[col] = None
                        
                # Reorder columns
                df = df[column_order]
                category_dfs[ntee_category] = df
        
        return category_dfs

    def write_to_excel(self, category_dfs):
        """Write data to Excel with append capability"""
        try:
            # Read existing data if file exists
            existing_data = self.read_existing_data()
            
            # Merge existing data with new data
            final_dfs = self.merge_data(existing_data, category_dfs)
            
            # Write to Excel
            with pd.ExcelWriter(self.output_path, engine='openpyxl', mode='w') as writer:
                for category, df in final_dfs.items():
                    sheet_name = self.clean_sheet_name(category)
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Format the worksheet
                    worksheet = writer.sheets[sheet_name]
                    
                    # Format columns
                    for idx, col in enumerate(df.columns):
                        # Set column width
                        max_length = max(
                            df[col].astype(str).apply(len).max(),
                            len(str(col))
                        )
                        col_letter = chr(65 + idx) if idx < 26 else chr(64 + idx//26) + chr(65 + (idx % 26))
                        worksheet.column_dimensions[col_letter].width = max_length + 2
                        
                        # Format numeric columns
                        if col not in ['Organization', 'Year']:
                            for row in range(2, len(df) + 2):  # Skip header
                                cell = worksheet.cell(row=row, column=idx + 1)
                                try:
                                    if pd.notna(cell.value):
                                        value = float(cell.value)
                                        cell.number_format = '#,##0'
                                except (ValueError, TypeError):
                                    continue
            
            return True
            
        except Exception as e:
            logger.error(f"Error writing to Excel: {str(e)}")
            raise

        
def main():
    # Initialize components
    parser = NonprofitParser()
    extractor = FinancialDataExtractor()
    excel_handler = ExcelOutputHandler(r'C:\Users\aronc\OneDrive\Documents\AppendSheet.xlsx')
    
    # Dictionary to store all org data by NTEE category
    all_org_data = {}
    
    while True:
        # Get organization URL from user
        org_url = input("\nEnter the ProPublica organization URL (or type 'done' to finish): ")
        
        if org_url.lower() == 'done':
            break
            
        try:
            # Get NTEE category and XML links
            ntee_category, xml_links = parser.scraper.get_organization_links(org_url)
            
            # Initialize category if needed
            if ntee_category not in all_org_data:
                all_org_data[ntee_category] = {}
            
            logger.info(f"\nProcessing organization in category: {ntee_category}")
            
            for url in xml_links:
                try:
                    # Parse basic information
                    result = parser.process_url(url)
                    org_name = result['organization_name']
                    
                    # Initialize organization in data structure if needed
                    if org_name not in all_org_data[ntee_category]:
                        all_org_data[ntee_category][org_name] = []
                    
                    # Extract financial data
                    result['financial_metrics'] = extractor.extract_financial_metrics(
                        result['parsed_content'],
                        result['format']
                    )
                    
                    # Extract executive compensation
                    result['executive_compensation'] = extractor.extract_executive_compensation(
                        result['parsed_content'],
                        result['format']
                    )
                    
                    # Extract endowment data (only for most recent year)
                    if xml_links.index(url) == 0:
                        result['endowment_data'] = extractor.extract_endowment_data(
                            result['parsed_content'],
                            result['format']
                        )
                    
                    all_org_data[ntee_category][org_name].append(result)
                    logger.info(f"Successfully processed {url}")
                    
                except Exception as e:
                    logger.error(f"Error processing {url}: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error processing organization: {str(e)}")
            continue
    
    # After all URLs are processed, write to Excel
    if all_org_data:
        category_dfs = excel_handler.consolidate_data(all_org_data)
        if excel_handler.write_to_excel(category_dfs):
            logger.info(f"\nSuccessfully wrote data to {excel_handler.output_path}")
        else:
            logger.error("Failed to write to Excel file")
    else:
        logger.error("No data was processed successfully")

if __name__ == "__main__":
    main()