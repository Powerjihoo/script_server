# Define variables

OS := $(if $(OS),$(OS),$(shell uname -s))
BUILD_TARGET := build_$(OS)
COMPILE_PB2_TARGET := compile_protobuf_$(OS)
SETUP_TARGET := setup_$(OS)

.PHONY: build $(BUILD_TARGET)
.PHONY: setup $(SETUP_TARGET)

build: $(BUILD_TARGET)

build_Windows_NT:
	@echo Detected OS: $(OS)
	@echo ALERT: Modified version information? \
If so, press any key to continue the build process. \
Otherwise, press Ctrl+C to stop and edit version_info.txt first, and then start again. \
(Need to change the version_info.txt file to ensure that the version information is displayed correctly.);
	@pause > nul
	@echo Starting to build program for Windows...
	@call scripts\build_window.bat
	conda env export > conda_environment.yml
	@make changelog

build_Linux:
	@export PROGRAMNAME=$(shell basename $(CURDIR)); \
	@echo "$$PROGRAMNAME"; \
	@echo "Detected OS: $(OS)"; \
	@read -p "Enter the version to build: " version; \
	@echo Starting to build program for Linux...; \
	docker build -t $$PROGRAMNAME:$$version .
	pip list --format=freeze > requirements.txt
	@make changelog

compile_protobuf: $(COMPILE_PB2_TARGET)

compile_protobuf_Linux:
	protoc --python_out=. ./src/_protobuf/script_data.proto
	
compile_protobuf_Windows_NT:
	include\protobuf\bin\protoc.exe --python_out=. .\src\_protobuf\script_data.proto

# install_protobuf_Linux:
	
# * Setup
setup: $(SETUP_TARGET)

setup_Linux:
	pip install -r requirements.txt

setup_Windows_NT:
	conda env create -f conda_environment.yml

changelog:
	set PYTHONIOENCODING=utf-8
	@gitchangelog > CHANGELOG.md
	@echo Updated CHANGELOG
	set PYTHONIOENCODING=