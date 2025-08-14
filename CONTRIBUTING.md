# 🤝 Contributing to DataHub Metadata Manager

Thank you for your interest in contributing to the DataHub Metadata Manager! This document provides guidelines and information for contributors.

## 🚀 Getting Started

### Prerequisites
- Python 3.8 or higher
- Git
- Access to a Trino instance (for testing)
- Access to a DataHub instance (for testing)

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/datahub-metadata-manager.git
   cd datahub-metadata-manager
   ```

2. **Set up Development Environment**
   ```bash
   python setup.py
   ```

3. **Configure Environment**
   ```bash
   cp .env.example .env
   # Edit .env with your development settings
   ```

4. **Run in Development Mode**
   ```bash
   python run.py
   ```

## 🎯 How to Contribute

### 🐛 Reporting Bugs

1. **Check existing issues** to avoid duplicates
2. **Use the bug report template** when creating new issues
3. **Include detailed information**:
   - Python version
   - Operating system
   - Steps to reproduce
   - Expected vs actual behavior
   - Screenshots if applicable

### ✨ Suggesting Features

1. **Check existing feature requests** to avoid duplicates
2. **Use the feature request template**
3. **Provide detailed description**:
   - Use case and motivation
   - Proposed solution
   - Alternative solutions considered
   - Additional context

### 🔧 Code Contributions

#### Branch Naming Convention
- `feature/description` - New features
- `bugfix/description` - Bug fixes
- `hotfix/description` - Critical fixes
- `docs/description` - Documentation updates

#### Development Workflow

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**
   - Follow the coding standards below
   - Add tests if applicable
   - Update documentation

3. **Test your changes**
   ```bash
   # Test application startup
   python run.py
   
   # Test key functionality
   # - Upload CSV
   # - Load schemas/tables
   # - Emit to DataHub (if possible)
   ```

4. **Commit your changes**
   ```bash
   git add .
   git commit -m "✨ Add feature: description of your changes"
   ```

5. **Push and create PR**
   ```bash
   git push origin feature/your-feature-name
   ```

## 📝 Coding Standards

### Python Code Style
- Follow PEP 8 guidelines
- Use meaningful variable and function names
- Add docstrings for functions and classes
- Keep functions focused and small
- Use type hints where appropriate

### Frontend Code Style
- Use consistent indentation (2 spaces for HTML/JS)
- Follow Bootstrap conventions for styling
- Use semantic HTML elements
- Add comments for complex JavaScript logic

### Commit Message Format
Use conventional commits format:
```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `✨ feat` - New feature
- `🐛 fix` - Bug fix
- `📚 docs` - Documentation
- `🎨 style` - Code style changes
- `♻️ refactor` - Code refactoring
- `⚡ perf` - Performance improvements
- `✅ test` - Adding tests
- `🔧 chore` - Maintenance tasks

## 🧪 Testing Guidelines

### Manual Testing Checklist
- [ ] Application starts without errors
- [ ] Can connect to Trino and load catalogs/schemas/tables
- [ ] Manual metadata entry works correctly
- [ ] CSV upload and validation works
- [ ] Auto-discovery dialog functions properly
- [ ] DataHub emission works (if DataHub available)
- [ ] UI is responsive and user-friendly
- [ ] Error handling provides clear messages

### Test Data
- Use the provided `sample_metadata.csv` for testing
- Test with various CSV formats and edge cases
- Test with different Trino schema structures

## 📋 Pull Request Guidelines

### Before Submitting
- [ ] Code follows the style guidelines
- [ ] Self-review of the code
- [ ] Manual testing completed
- [ ] Documentation updated if needed
- [ ] No merge conflicts with main branch

### PR Description Template
```markdown
## 🎯 Description
Brief description of changes

## 🔄 Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Documentation update
- [ ] Performance improvement
- [ ] Other (please describe)

## 🧪 Testing
- [ ] Manual testing completed
- [ ] Edge cases considered
- [ ] Error scenarios tested

## 📸 Screenshots (if applicable)
Add screenshots for UI changes

## 📝 Additional Notes
Any additional information or context
```

## 🏗️ Architecture Overview

### Key Components
- **`app.py`** - Main Flask application with routes
- **`config.py`** - Configuration management
- **`templates/`** - HTML templates with Bootstrap styling
- **Frontend JavaScript** - Dynamic UI interactions and AJAX calls

### Key Features
- **Trino Integration** - Catalog/schema/table browsing
- **DataHub Integration** - Metadata emission with proper entities
- **CSV Processing** - Bulk metadata upload with validation
- **Auto-discovery** - Smart loading of missing schemas/tables
- **Session Management** - Clean data handling and validation

## 🎨 UI/UX Guidelines

### Design Principles
- **User-friendly** - Clear navigation and feedback
- **Professional** - Clean, modern interface
- **Responsive** - Works on different screen sizes
- **Accessible** - Proper contrast and semantic HTML

### Component Guidelines
- Use Bootstrap components consistently
- Provide visual feedback for all actions
- Show loading states for async operations
- Use icons to enhance understanding
- Implement proper error handling and messages

## 🚀 Release Process

### Version Numbering
We follow Semantic Versioning (SemVer):
- **MAJOR** - Breaking changes
- **MINOR** - New features (backward compatible)
- **PATCH** - Bug fixes (backward compatible)

### Release Checklist
- [ ] All tests pass
- [ ] Documentation updated
- [ ] CHANGELOG.md updated
- [ ] Version bumped in relevant files
- [ ] Release notes prepared

## 📞 Getting Help

### Community
- **GitHub Issues** - For bugs and feature requests
- **GitHub Discussions** - For questions and general discussion

### Maintainers
- Review PRs and issues
- Provide guidance on architecture decisions
- Maintain code quality standards
- Coordinate releases

## 🙏 Recognition

Contributors will be recognized in:
- README.md contributors section
- Release notes
- GitHub contributors page

Thank you for contributing to making DataHub metadata management better for everyone! 🎉