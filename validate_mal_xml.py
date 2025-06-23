import gzip
import sys
import argparse
from lxml import etree

def validate_mal_xml(xml_path, xsd_path):
    """
    Validate a MyAnimeList XML file (gzipped or plain) against an XSD schema.
    """
    try:
        # Load and parse the XSD schema
        print("Loading XSD schema...")
        with open(xsd_path, 'r', encoding='utf-8') as xsd_file:
            xsd_doc = etree.parse(xsd_file, parser=etree.XMLParser(remove_blank_text=True))
            schema = etree.XMLSchema(xsd_doc)
        
        # Determine if file is gzipped based on extension
        is_gzipped = xml_path.lower().endswith('.gz')
        
        # Extract and parse the XML file
        if is_gzipped:
            print("Extracting and parsing gzipped XML file...")
            with gzip.open(xml_path, 'rt', encoding='utf-8') as xml_file:
                xml_doc = etree.parse(xml_file, parser=etree.XMLParser(remove_blank_text=True))
        else:
            print("Parsing XML file...")
            with open(xml_path, 'r', encoding='utf-8') as xml_file:
                xml_doc = etree.parse(xml_file, parser=etree.XMLParser(remove_blank_text=True))
        
        # Validate the XML against the schema
        print("Validating XML against schema...")
        is_valid = schema.validate(xml_doc)
        
        if is_valid:
            print("✅ XML file is valid according to the XSD schema!")
            
            # Print some basic statistics
            root = xml_doc.getroot()
            anime_count = len(root.findall('anime'))
            user_total = root.find('.//user_total_anime')
            if user_total is not None:
                print(f"📊 Found {anime_count} anime entries")
                print(f"📊 User total anime: {user_total.text}")
        else:
            print("❌ XML file is NOT valid according to the XSD schema!")
            print("\nValidation errors:")
            for error in schema.error_log:
                print(f"  Line {error.line}: {error.message}")
        
        return is_valid
        
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        return False
    except etree.XMLSyntaxError as e:
        print(f"❌ XML syntax error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error during validation: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Validate MyAnimeList XML export against XSD schema')
    parser.add_argument('xml_file', help='Path to the XML file to validate (.xml or .xml.gz)')
    parser.add_argument('--xsd', default='myanimelist.xsd', help='Path to XSD schema file (default: myanimelist.xsd)')
    
    args = parser.parse_args()
    
    print("MyAnimeList XML Validator")
    print("=" * 40)
    print(f"XML file: {args.xml_file}")
    print(f"XSD file: {args.xsd}")
    print("-" * 40)
    
    is_valid = validate_mal_xml(args.xml_file, args.xsd)
    
    sys.exit(0 if is_valid else 1)

if __name__ == "__main__":
    main()
