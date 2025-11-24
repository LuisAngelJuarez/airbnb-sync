from app import create_app

app = create_app()

if __name__ == "__main__":
    # Para pruebas locales
    app.run(host="0.0.0.0", port=8080)
