#!/bin/bash
# ============================================================================
# KMD WORKSHOP - MASTER DEPLOYMENT SCRIPT
# ============================================================================
# Deploys all objects in correct order
# 
# Prerequisites:
#   - Snowflake CLI (snow) installed and configured
#   - Storage integration KMD_S3_INTEGRATION created manually
#   - Connection named 'kmd_demo' configured (or update CONNECTION below)
# ============================================================================

CONNECTION="kmd_demo"

echo "=============================================="
echo "KMD Workshop Deployment"
echo "=============================================="

echo ""
echo "[1/8] Creating databases and schemas..."
snow sql -f deploy/01_databases_schemas.sql -c $CONNECTION

echo ""
echo "[2/8] Creating external stages..."
snow sql -f deploy/02_external_stages.sql -c $CONNECTION

echo ""
echo "[3/8] Creating RAW tables and loading data..."
snow sql -f deploy/03_raw_tables_load.sql -c $CONNECTION

echo ""
echo "[4/8] Setting up security (roles, RLS, masking)..."
snow sql -f deploy/04_security.sql -c $CONNECTION

echo ""
echo "[5/8] Creating tenant views..."
snow sql -f deploy/05_tenant_views.sql -c $CONNECTION

echo ""
echo "[6/8] Creating streams and tasks..."
snow sql -f deploy/06_streams_tasks.sql -c $CONNECTION

echo ""
echo "[7/8] Creating dynamic tables..."
snow sql -f deploy/07_dynamic_tables.sql -c $CONNECTION

echo ""
echo "[8/8] Creating semantic view..."
snow sql -f deploy/08_semantic_view.sql -c $CONNECTION

echo ""
echo "=============================================="
echo "Deployment complete!"
echo "=============================================="
echo ""
echo "Verify deployment:"
echo "  - Databases: KMD_SCHOOLS, KMD_STAGING, KMD_ANALYTICS"
echo "  - Stages: 7 external stages in KMD_STAGING.EXTERNAL_STAGES"
echo "  - Tables: 4 RAW tables with data loaded"
echo "  - Security: 6 roles, 4 masking policies, 1 RLS policy"
echo "  - Views: 20 tenant views (4 per municipality)"
echo "  - Streams: 4 streams"
echo "  - Tasks: 4 tasks (suspended)"
echo "  - Dynamic Tables: 3"
echo "  - Semantic View: KMD_ANALYTICS.SEMANTIC_MODELS.KMD_SCHOOLS_ANALYTICS"
echo ""
