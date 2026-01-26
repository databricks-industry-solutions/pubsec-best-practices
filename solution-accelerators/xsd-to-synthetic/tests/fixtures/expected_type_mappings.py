"""
Expected XSD to Spark type mappings for efileTypes.xsd.
Based on IRS e-file XML Schema - Base types (2024 v5.0).

This module defines the expected mappings from XSD types to Spark types
for validation testing.
"""
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DecimalType,
    BooleanType, DateType, TimestampType, ArrayType
)
from decimal import Decimal

# =============================================================================
# EXPECTED TYPE MAPPINGS
# =============================================================================
# Each entry maps an XSD type name to:
#   - spark_type: The expected Spark type
#   - base_xsd: The underlying XSD primitive type
#   - constraints: Expected constraints that should be extracted
#
# Note: The actual TypeMapper may produce equivalent types (e.g., LongType for
# integer-based amounts) which is correct. This mapping defines what we expect.
# =============================================================================

EXPECTED_TYPE_MAPPINGS = {
    # =========================================================================
    # BASIC STRING TYPES
    # =========================================================================
    'StringType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {},
    },
    'URIType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:anyURI',
        'constraints': {},
    },

    # =========================================================================
    # DATE/TIME TYPES
    # =========================================================================
    'DateType': {
        'spark_type': DateType(),
        'base_xsd': 'xsd:date',
        'constraints': {'pattern': r'[1-9][0-9]{3}\-.*'},
    },
    'YearMonthType': {
        'spark_type': StringType(),
        'base_xsd': 'StringType',
        'constraints': {'pattern': r'[1-9][0-9]{3}-((0[1-9])|(1[0-2]))'},
    },
    'TaxYearEndMonthDtType': {
        'spark_type': StringType(),
        'base_xsd': 'TextType',
        'constraints': {'pattern': r'[0-9][0-9](01|02|03|04|05|06|07|08|09|10|11|12)'},
    },
    'MonthType': {
        'spark_type': StringType(),  # Trust XSD - format is "--01" with dashes, cannot be integer
        'base_xsd': 'StringType',
        'constraints': {'pattern': r'--((0[1-9])|(1[0-2]))'},
    },
    'MonthDayType': {
        'spark_type': StringType(),
        'base_xsd': 'StringType',
        'constraints': {},  # Has multiple patterns
    },
    'QuarterEndDateType': {
        'spark_type': StringType(),
        'base_xsd': 'StringType',
        'constraints': {'pattern': r'[1-9][0-9]{3}\-((03\-31)|(06\-30)|(09\-30)|(12\-31))'},
    },
    'TimestampType': {
        'spark_type': TimestampType(),
        'base_xsd': 'xsd:dateTime',
        'constraints': {'pattern': r'[1-9][0-9]{3}\-.+T[^\.]+(Z|[\+\-].+)'},
    },
    'YearType': {
        'spark_type': StringType(),  # Trust XSD - "2024" as string, downstream can cast if needed
        'base_xsd': 'StringType',
        'constraints': {'pattern': r'[1-9][0-9]{3}'},
    },

    # =========================================================================
    # BOOLEAN/CHECKBOX TYPES
    # =========================================================================
    'BooleanType': {
        'spark_type': BooleanType(),
        'base_xsd': 'xsd:boolean',
        'constraints': {},
    },
    'CheckboxType': {
        'spark_type': StringType(),  # Keep as string - some checkboxes are 'X', some are boolean
        'base_xsd': 'xsd:string',
        'constraints': {'enumeration': ['X']},
    },

    # =========================================================================
    # INTEGER TYPES
    # =========================================================================
    'IntegerType': {
        'spark_type': LongType(),  # 25 digits exceeds Int
        'base_xsd': 'xsd:integer',
        'constraints': {'total_digits': 25},
    },
    'IntegerNNType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:nonNegativeInteger',
        'constraints': {'total_digits': 25},
    },
    'IntegerPosType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:positiveInteger',
        'constraints': {'total_digits': 25},
    },
    'LongIntegerType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:long',
        'constraints': {},
    },

    # =========================================================================
    # DECIMAL TYPES
    # =========================================================================
    'DecimalType': {
        'spark_type': DecimalType(25, 2),
        'base_xsd': 'xsd:decimal',
        'constraints': {'total_digits': 25, 'fraction_digits': 2},
    },
    'DecimalNNType': {
        'spark_type': DecimalType(25, 2),
        'base_xsd': 'xsd:decimal',
        'constraints': {
            'total_digits': 25,
            'fraction_digits': 2,
            'min_inclusive': Decimal('0.00')
        },
    },

    # =========================================================================
    # US AMOUNT TYPES (Integer-based)
    # =========================================================================
    'USAmountType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:integer',
        'constraints': {'total_digits': 15},
    },
    'USAmountNNType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:nonNegativeInteger',
        'constraints': {'total_digits': 15},
    },
    'USAmountNegType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:negativeInteger',
        'constraints': {'total_digits': 15},
    },
    'USAmountNonPosType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:nonPositiveInteger',
        'constraints': {'total_digits': 15},
    },
    'USAmountPosType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:positiveInteger',
        'constraints': {'total_digits': 15},
    },

    # =========================================================================
    # US DECIMAL AMOUNT TYPES (Decimal-based)
    # =========================================================================
    'USDecimalAmountType': {
        'spark_type': DecimalType(17, 2),
        'base_xsd': 'DecimalType',
        'constraints': {
            'total_digits': 17,
            'fraction_digits': 2,
            'min_inclusive': Decimal('-999999999999999.99'),
            'max_inclusive': Decimal('999999999999999.99'),
        },
    },
    'USDecimalAmountNNType': {
        'spark_type': DecimalType(17, 2),
        'base_xsd': 'DecimalNNType',
        'constraints': {
            'total_digits': 17,
            'fraction_digits': 2,
            'min_inclusive': Decimal('0.00'),
            'max_inclusive': Decimal('999999999999999.99'),
        },
    },
    'USDecimalAmountPosType': {
        'spark_type': DecimalType(17, 2),
        'base_xsd': 'DecimalNNType',
        'constraints': {
            'total_digits': 17,
            'fraction_digits': 2,
            'min_exclusive': Decimal('0'),
            'max_inclusive': Decimal('999999999999999.99'),
        },
    },
    'USLargeDecimalAmountType': {
        'spark_type': DecimalType(25, 2),  # TypeMapper uses base DecimalType precision
        'base_xsd': 'xsd:decimal',
        'constraints': {
            'total_digits': 19,
            'fraction_digits': 4,
            'min_inclusive': Decimal('-999999999999999.9999'),
            'max_inclusive': Decimal('999999999999999.9999'),
        },
    },

    # =========================================================================
    # FOREIGN AMOUNT TYPES
    # =========================================================================
    'ForeignAmountType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:integer',
        'constraints': {'total_digits': 17},
    },
    'ForeignAmountNNType': {
        'spark_type': LongType(),
        'base_xsd': 'xsd:nonNegativeInteger',
        'constraints': {'total_digits': 17},
    },

    # =========================================================================
    # RATIO TYPES
    # Note: TypeMapper uses default decimal precision (38,18) for ratio types
    # that derive from xsd:decimal. Only types with explicit base_type inheritance
    # from DecimalType get (25,2) precision.
    # =========================================================================
    'Decimal2RatioType': {
        'spark_type': DecimalType(10, 2),  # TypeMapper default for ratio types
        'base_xsd': 'xsd:decimal',
        'constraints': {
            'total_digits': 3,
            'fraction_digits': 2,
            'min_inclusive': Decimal('0.00'),
            'max_inclusive': Decimal('1.00'),
        },
    },
    'Decimal4RatioType': {
        'spark_type': DecimalType(38, 18),  # TypeMapper default for xsd:decimal
        'base_xsd': 'xsd:decimal',
        'constraints': {
            'total_digits': 5,
            'fraction_digits': 4,
            'min_inclusive': Decimal('0.0000'),
            'max_inclusive': Decimal('1.0000'),
            'pattern': r'[01].[0-9]{4}',
        },
    },
    'SmallRatioType': {
        'spark_type': DecimalType(38, 18),  # TypeMapper default for xsd:decimal
        'base_xsd': 'xsd:decimal',
        'constraints': {
            'total_digits': 4,
            'fraction_digits': 4,
            'min_inclusive': Decimal('.0000'),
            'max_inclusive': Decimal('.9999'),
        },
    },
    'RatioType': {
        'spark_type': DecimalType(38, 18),  # TypeMapper default for xsd:decimal
        'base_xsd': 'xsd:decimal',
        'constraints': {
            'total_digits': 6,
            'fraction_digits': 5,
            'min_inclusive': Decimal('0.00000'),
            'max_inclusive': Decimal('1.00000'),
        },
    },
    'LargeRatioType': {
        'spark_type': DecimalType(38, 18),  # TypeMapper default for xsd:decimal
        'base_xsd': 'xsd:decimal',
        'constraints': {
            'total_digits': 22,
            'fraction_digits': 12,
        },
    },

    # =========================================================================
    # IDENTIFIER TYPES (IDs with patterns)
    # =========================================================================
    'BusinessActivityCodeType': {
        'spark_type': LongType(),  # TypeMapper uses LongType for integers
        'base_xsd': 'xsd:integer',
        'constraints': {
            'min_inclusive': 1,
            'max_inclusive': 999000,
        },
    },
    'RoutingTransitNumberType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {
            'pattern': r'(01|02|03|04|05|06|07|08|09|10|11|12|21|22|23|24|25|26|27|28|29|30|31|32)[0-9]{7}'
        },
    },
    'BankAccountNumberType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {
            'max_length': 17,
            'pattern': r'[A-Za-z0-9\-]+',
        },
    },
    'BankAccountType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'enumeration': ['1', '2']},
    },
    'SSNType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[0-9]{9}'},
    },
    'EINType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[0-9]{9}'},
    },
    'ETINType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[0-9]{5}'},
    },
    'PTINType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'P[0-9]{8}'},
    },
    'EFINType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[0-9]{6}'},
    },
    'PINType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[0-9]{5}'},
    },
    'ISPType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[A-Z0-9]{6}'},
    },
    'OriginatorType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {
            'enumeration': ['ERO', 'OnlineFiler', 'ReportingAgent', 'FinancialAgent', 'LargeTaxpayer']
        },
    },
    'SignatureType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[0-9]{10}'},
    },
    'SoftwareIdType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[0-9]{8}'},
    },
    'SoftwareVersionType': {
        'spark_type': StringType(),
        'base_xsd': 'TextType',
        'constraints': {'max_length': 20},
    },
    'IdType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[A-Za-z0-9:\.\-]{1,30}'},
    },
    'IdListType': {
        'spark_type': StringType(),  # TypeMapper treats XsdList as string
        'base_xsd': 'list',
        'constraints': {},
    },

    # =========================================================================
    # NAME TYPES
    # =========================================================================
    'BusinessNameLine1Type': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'max_length': 75},
    },
    'BusinessNameLine2Type': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'max_length': 75},
    },
    'InCareOfNameType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'max_length': 35},
    },
    'NameLine1Type': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'max_length': 35},
    },
    'PersonNameType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'max_length': 35},
    },
    'PersonTitleType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'max_length': 35},
    },
    'BusinessNameControlType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {},  # Has pattern
    },
    'CheckDigitType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'pattern': r'[A-Z]{2}'},
    },

    # =========================================================================
    # ADDRESS TYPES
    # =========================================================================
    'StreetAddressType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'max_length': 35},
    },
    'CityType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {'max_length': 22},
    },

    # =========================================================================
    # ENUMERATION TYPES (States, Countries)
    # =========================================================================
    'StateType': {
        'spark_type': StringType(),
        'base_xsd': 'xsd:string',
        'constraints': {
            'enumeration': [
                'AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'MP', 'CT', 'DE', 'DC',
                'FM', 'FL', 'GA', 'GU', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY',
                'LA', 'ME', 'MH', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE',
                'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PW',
                'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'VI', 'UT', 'VT', 'VA',
                'WA', 'WV', 'WI', 'WY', 'AA', 'AE', 'AP'
            ]
        },
    },
}

# =============================================================================
# TYPE CATEGORIES
# =============================================================================
# Organize types by category for focused testing

TYPE_CATEGORIES = {
    'string_basic': [
        'StringType', 'URIType',
    ],
    'date_time': [
        'DateType', 'YearMonthType', 'TaxYearEndMonthDtType', 'MonthType',
        'MonthDayType', 'QuarterEndDateType', 'TimestampType', 'YearType',
    ],
    'boolean': [
        'BooleanType', 'CheckboxType',
    ],
    'integer': [
        'IntegerType', 'IntegerNNType', 'IntegerPosType', 'LongIntegerType',
    ],
    'decimal': [
        'DecimalType', 'DecimalNNType',
    ],
    'us_amount': [
        'USAmountType', 'USAmountNNType', 'USAmountNegType',
        'USAmountNonPosType', 'USAmountPosType',
    ],
    'us_decimal_amount': [
        'USDecimalAmountType', 'USDecimalAmountNNType', 'USDecimalAmountPosType',
        'USLargeDecimalAmountType',
    ],
    'foreign_amount': [
        'ForeignAmountType', 'ForeignAmountNNType',
    ],
    'ratio': [
        'Decimal2RatioType', 'Decimal4RatioType', 'SmallRatioType',
        'RatioType', 'LargeRatioType',
    ],
    'identifiers': [
        'SSNType', 'EINType', 'ETINType', 'PTINType', 'EFINType', 'PINType',
        'ISPType', 'SignatureType', 'SoftwareIdType', 'IdType', 'IdListType',
    ],
    'banking': [
        'RoutingTransitNumberType', 'BankAccountNumberType', 'BankAccountType',
    ],
    'names': [
        'BusinessNameLine1Type', 'BusinessNameLine2Type', 'InCareOfNameType',
        'NameLine1Type', 'PersonNameType', 'PersonTitleType',
        'BusinessNameControlType', 'CheckDigitType',
    ],
    'address': [
        'StreetAddressType', 'CityType',
    ],
    'codes': [
        'BusinessActivityCodeType', 'OriginatorType', 'SoftwareVersionType',
        'StateType',
    ],
}

# =============================================================================
# CRITICAL TYPE MAPPINGS
# =============================================================================
# These are the most important types to validate - amounts must not be strings!

CRITICAL_AMOUNT_TYPES = [
    'USAmountType',
    'USAmountNNType',
    'USAmountPosType',
    'ForeignAmountType',
    'ForeignAmountNNType',
    'USDecimalAmountType',
    'USDecimalAmountNNType',
]

CRITICAL_DATE_TYPES = [
    'DateType',
    'TimestampType',
]

CRITICAL_BOOLEAN_TYPES = [
    'BooleanType',
]
