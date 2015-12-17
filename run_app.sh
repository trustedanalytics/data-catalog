#!/bin/bash
gunicorn 'data_catalog.app:get_app()' --bind :5000 --enable-stdio-inheritance
