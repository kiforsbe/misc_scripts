"""
CBR to CBZ Converter
Converts Comic Book RAR (CBR) files to Comic Book ZIP (CBZ) files using in-memory processing.
Uses libarchive-c to avoid extracting files to disk, optimized for slow USB drives.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional
import zipfile
import io

# Try to import libarchive (preferred method)
LIBARCHIVE_AVAILABLE = False
RARFILE_AVAILABLE = False

try:
    import libarchive
    LIBARCHIVE_AVAILABLE = True
except (ImportError, OSError, TypeError) as e:
    # libarchive-c not installed or DLL not found/loadable
    pass

# Try to import rarfile as fallback
if not LIBARCHIVE_AVAILABLE:
    try:
        import rarfile
        RARFILE_AVAILABLE = True
    except ImportError:
        pass

# Check if we have at least one extraction method
if not LIBARCHIVE_AVAILABLE and not RARFILE_AVAILABLE:
    print("Error: No CBR extraction library available.")
    print("\nOption 1 (recommended): Install libarchive-c")
    print(f"  {sys.executable} -m pip install libarchive-c")
    print("  Note: Requires libarchive DLL to be installed on your system")
    print("\nOption 2 (fallback): Install rarfile")
    print(f"  {sys.executable} -m pip install rarfile")
    print("  Note: Requires UnRAR tool to be installed and on PATH")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print(f"Error: tqdm is not installed.")
    print(f"\nInstall it with: {sys.executable} -m pip install tqdm")
    sys.exit(1)


class CBRtoCBZConverter:
    """Converts CBR files to CBZ format using in-memory processing."""
    
    def __init__(self, log_level: int = logging.INFO):
        """
        Initialize the converter.
        
        Args:
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)
        
        # Setup console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        
        # Log which extraction library is being used
        if LIBARCHIVE_AVAILABLE:
            self.logger.info("Using libarchive for CBR extraction")
        elif RARFILE_AVAILABLE:
            self.logger.info("Using rarfile for CBR extraction")
        
        self.stats = {
            'total': 0,
            'converted': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def find_cbr_files(self, root_path: Path) -> List[Path]:
        """
        Recursively find all CBR files in the given directory.
        
        Args:
            root_path: Root directory to search
            
        Returns:
            List of Path objects for CBR files, sorted by path
        """
        self.logger.info(f"Scanning for CBR files in: {root_path}")
        cbr_files = list(root_path.rglob("*.cbr"))
        cbr_files.extend(root_path.rglob("*.CBR"))
        
        # Remove duplicates (case-insensitive on Windows)
        unique_files = list(set(cbr_files))
        
        # Sort files by full path for logical ordering
        unique_files.sort()
        
        self.logger.info(f"Found {len(unique_files)} CBR file(s)")
        return unique_files
    
    def convert_cbr_to_cbz(self, cbr_path: Path, delete_original: bool = False) -> bool:
        """
        Convert a single CBR file to CBZ format using in-memory processing.
        
        Args:
            cbr_path: Path to the CBR file
            delete_original: Whether to delete the original CBR file after successful conversion
            
        Returns:
            True if conversion was successful, False otherwise
        """
        cbz_path = cbr_path.with_suffix('.cbz')
        
        # Check if CBZ already exists
        if cbz_path.exists():
            self.logger.warning(f"CBZ already exists, skipping: {cbz_path.name}")
            self.stats['skipped'] += 1
            return False
        
        self.logger.debug(f"Converting: {cbr_path.name}")
        
        try:
            # Create an in-memory buffer for the ZIP file
            zip_buffer = io.BytesIO()
            file_count = 0
            
            # Try libarchive first (preferred, faster)
            if LIBARCHIVE_AVAILABLE:
                try:
                    self.logger.debug(f"  Using libarchive for: {cbr_path.name}")
                    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                        with libarchive.file_reader(str(cbr_path)) as archive:
                            for entry in archive:
                                # Skip directories
                                if entry.isdir:
                                    continue
                                
                                # Get entry name
                                entry_name = entry.pathname if hasattr(entry, 'pathname') else entry.name
                                if not entry_name:
                                    continue
                                
                                # Read file data from archive
                                file_data = b''.join(entry.get_blocks())
                                
                                # Write to ZIP
                                zip_file.writestr(entry_name, file_data)
                                file_count += 1
                                
                                self.logger.debug(f"  Added: {entry_name} ({len(file_data)} bytes)")
                except Exception as e:
                    if RARFILE_AVAILABLE:
                        self.logger.debug(f"  libarchive failed, trying rarfile: {e}")
                        zip_buffer = io.BytesIO()
                        file_count = 0
                    else:
                        raise
            
            # Use rarfile if libarchive not available or failed
            if (not LIBARCHIVE_AVAILABLE or file_count == 0) and RARFILE_AVAILABLE:
                self.logger.debug(f"  Using rarfile for: {cbr_path.name}")
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                    with rarfile.RarFile(str(cbr_path)) as rar:
                        for member in rar.infolist():
                            # Skip directories
                            if member.isdir():
                                continue
                            
                            # Read file data from RAR
                            file_data = rar.read(member)
                            
                            # Write to ZIP
                            zip_file.writestr(member.filename, file_data)
                            file_count += 1
                            
                            self.logger.debug(f"  Added: {member.filename} ({len(file_data)} bytes)")
            
            if file_count == 0:
                raise Exception("No files extracted from CBR archive")
            
            # Write the ZIP buffer to disk
            with open(cbz_path, 'wb') as f:
                f.write(zip_buffer.getvalue())
            
            self.logger.info(f"✓ Converted: {cbr_path.name} ({file_count} files)")
            self.stats['converted'] += 1
            
            # Delete original if requested
            if delete_original:
                cbr_path.unlink()
                self.logger.debug(f"  Deleted original: {cbr_path.name}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Failed to convert {cbr_path.name}: {str(e)}")
            self.stats['failed'] += 1
            
            # Clean up partial CBZ file if it exists
            if cbz_path.exists():
                try:
                    cbz_path.unlink()
                    self.logger.debug(f"  Cleaned up partial file: {cbz_path.name}")
                except Exception as cleanup_error:
                    self.logger.warning(f"  Could not clean up partial file: {cleanup_error}")
            
            return False
    
    def convert_directory(self, root_path: Path, delete_original: bool = False) -> None:
        """
        Convert all CBR files in a directory tree.
        
        Args:
            root_path: Root directory to process
            delete_original: Whether to delete original CBR files after successful conversion
        """
        cbr_files = self.find_cbr_files(root_path)
        self.stats['total'] = len(cbr_files)
        
        if not cbr_files:
            self.logger.warning("No CBR files found")
            return
        
        # Print initial summary
        print("\n" + "=" * 50)
        print("CONVERSION START")
        print("=" * 50)
        print(f"Root directory:       {root_path}")
        print(f"Total CBR files:      {len(cbr_files)}")
        print(f"Delete originals:     {'Yes' if delete_original else 'No'}")
        extraction_method = "libarchive" if LIBARCHIVE_AVAILABLE else "rarfile"
        print(f"Extraction method:    {extraction_method}")
        print("=" * 50 + "\n")
        
        self.logger.info(f"Starting conversion of {len(cbr_files)} file(s)...")
        
        # Process files with progress bar
        with tqdm(cbr_files, desc="Converting", unit="file") as pbar:
            for cbr_file in pbar:
                pbar.set_description(f"Converting {cbr_file.name[:30]}")
                self.convert_cbr_to_cbz(cbr_file, delete_original)
        
        # Print summary
        self.print_summary()
    
    def print_summary(self) -> None:
        """Print conversion statistics."""
        print("\n" + "=" * 50)
        print("CONVERSION SUMMARY")
        print("=" * 50)
        print(f"Total files found:    {self.stats['total']}")
        print(f"Successfully converted: {self.stats['converted']}")
        print(f"Failed:               {self.stats['failed']}")
        print(f"Skipped (already exist): {self.stats['skipped']}")
        print("=" * 50)


def setup_argparse() -> argparse.ArgumentParser:
    """Setup command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Convert CBR (Comic Book RAR) files to CBZ (Comic Book ZIP) format using in-memory processing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/comics
  %(prog)s /path/to/comics --keep-original
  %(prog)s /path/to/comics -v
  %(prog)s /path/to/comics -vv
  %(prog)s /path/to/comics --quiet
        """
    )
    
    parser.add_argument(
        'root_path',
        type=Path,
        help='Root directory to search for CBR files'
    )
    
    parser.add_argument(
        '--keep-original',
        action='store_true',
        help='Keep original CBR files after conversion (default: delete after successful conversion)'
    )
    
    # Verbosity control
    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Increase verbosity (-v for INFO, -vv for DEBUG)'
    )
    verbosity_group.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Suppress all output except errors'
    )
    
    return parser


def get_log_level(verbose: int, quiet: bool) -> int:
    """
    Determine logging level based on verbosity flags.
    
    Args:
        verbose: Verbosity count (0, 1, or 2+)
        quiet: Whether quiet mode is enabled
        
    Returns:
        Logging level constant
    """
    if quiet:
        return logging.ERROR
    elif verbose >= 2:
        return logging.DEBUG
    elif verbose == 1:
        return logging.INFO
    else:
        return logging.WARNING


def main():
    """Main entry point."""
    parser = setup_argparse()
    args = parser.parse_args()
    
    # Validate root path
    root_path = args.root_path.resolve()
    if not root_path.exists():
        print(f"Error: Path does not exist: {root_path}", file=sys.stderr)
        sys.exit(1)
    
    if not root_path.is_dir():
        print(f"Error: Path is not a directory: {root_path}", file=sys.stderr)
        sys.exit(1)
    
    # Determine log level
    log_level = get_log_level(args.verbose, args.quiet)
    
    # Create converter and run
    converter = CBRtoCBZConverter(log_level=log_level)
    
    try:
        # By default, delete originals unless --keep-original is specified
        delete_original = not args.keep_original
        converter.convert_directory(root_path, delete_original=delete_original)
    except KeyboardInterrupt:
        print("\n\nConversion interrupted by user")
        converter.print_summary()
        sys.exit(1)
    except Exception as e:
        converter.logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
