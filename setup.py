#!/usr/bin/env python3
"""
Setup script for DataHub Metadata Manager
"""
import os
import sys
import subprocess

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e.stderr}")
        return False

def main():
    print("🚀 DataHub Metadata Manager Setup")
    print("=" * 50)
    
    # Check Python version
    if sys.version_info < (3, 7):
        print("❌ Python 3.7 or higher is required")
        sys.exit(1)
    
    print(f"✅ Python {sys.version.split()[0]} detected")
    
    # Create virtual environment
    if not os.path.exists('venv'):
        if not run_command('python -m venv venv', 'Creating virtual environment'):
            sys.exit(1)
    else:
        print("✅ Virtual environment already exists")
    
    # Activate virtual environment and install dependencies
    if os.name == 'nt':  # Windows
        activate_cmd = 'venv\\Scripts\\activate && '
    else:  # Linux/Mac
        activate_cmd = 'source venv/bin/activate && '
    
    if not run_command(f'{activate_cmd}pip install -r requirements.txt', 'Installing dependencies'):
        sys.exit(1)
    
    # Create .env file if it doesn't exist
    if not os.path.exists('.env'):
        if os.path.exists('.env.example'):
            if os.name == 'nt':
                run_command('copy .env.example .env', 'Creating .env file')
            else:
                run_command('cp .env.example .env', 'Creating .env file')
            print("📝 Please edit .env file with your configuration")
        else:
            print("⚠️  .env.example not found, please create .env manually")
    else:
        print("✅ .env file already exists")
    
    # Create uploads directory
    os.makedirs('uploads', exist_ok=True)
    print("✅ Uploads directory ready")
    
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Edit .env file with your Trino and DataHub settings")
    print("2. Activate virtual environment:")
    if os.name == 'nt':
        print("   venv\\Scripts\\activate")
    else:
        print("   source venv/bin/activate")
    print("3. Run the application:")
    print("   python run.py")
    print("4. Open http://localhost:5000 in your browser")

if __name__ == '__main__':
    main()