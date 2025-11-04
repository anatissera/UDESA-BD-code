import random
import datetime
import os

# Configuración
TARGET_SIZE_GB = 2
TARGET_SIZE_BYTES = TARGET_SIZE_GB * 1024 * 1024 * 1024
OUTPUT_FILE = "spotify_logs.txt"

# Lista de 1000 canciones actuales con sus artistas reales
songs = [
    # Taylor Swift
    ("Anti-Hero", "Taylor Swift"),
    ("Lavender Haze", "Taylor Swift"),
    ("Cruel Summer", "Taylor Swift"),
    ("Blank Space", "Taylor Swift"),
    ("Shake It Off", "Taylor Swift"),
    ("All Too Well", "Taylor Swift"),
    ("Karma", "Taylor Swift"),
    ("You Belong With Me", "Taylor Swift"),
    ("Love Story", "Taylor Swift"),
    ("Wildest Dreams", "Taylor Swift"),

    # Bad Bunny
    ("Tití Me Preguntó", "Bad Bunny"),
    ("Moscow Mule", "Bad Bunny"),
    ("Callaíta", "Bad Bunny"),
    ("Dákiti", "Bad Bunny"),
    ("Yo Perreo Sola", "Bad Bunny"),
    ("Me Porto Bonito", "Bad Bunny"),
    ("Efecto", "Bad Bunny"),
    ("Un x100to", "Bad Bunny"),
    ("Ojitos Lindos", "Bad Bunny"),
    ("Party", "Bad Bunny"),

    # The Weeknd
    ("Blinding Lights", "The Weeknd"),
    ("Starboy", "The Weeknd"),
    ("Save Your Tears", "The Weeknd"),
    ("Die For You", "The Weeknd"),
    ("The Hills", "The Weeknd"),
    ("Can't Feel My Face", "The Weeknd"),
    ("I Feel It Coming", "The Weeknd"),
    ("After Hours", "The Weeknd"),
    ("Sacrifice", "The Weeknd"),
    ("Out of Time", "The Weeknd"),

    # Drake
    ("God's Plan", "Drake"),
    ("One Dance", "Drake"),
    ("Hotline Bling", "Drake"),
    ("In My Feelings", "Drake"),
    ("Nice For What", "Drake"),
    ("Passionfruit", "Drake"),
    ("Started From The Bottom", "Drake"),
    ("Hold On We're Going Home", "Drake"),
    ("Too Good", "Drake"),
    ("Fake Love", "Drake"),

    # Ed Sheeran
    ("Shape of You", "Ed Sheeran"),
    ("Perfect", "Ed Sheeran"),
    ("Thinking Out Loud", "Ed Sheeran"),
    ("Shivers", "Ed Sheeran"),
    ("Bad Habits", "Ed Sheeran"),
    ("Photograph", "Ed Sheeran"),
    ("Castle on the Hill", "Ed Sheeran"),
    ("Galway Girl", "Ed Sheeran"),
    ("Beautiful People", "Ed Sheeran"),
    ("I Don't Care", "Ed Sheeran"),

    # Billie Eilish
    ("bad guy", "Billie Eilish"),
    ("Happier Than Ever", "Billie Eilish"),
    ("everything i wanted", "Billie Eilish"),
    ("ocean eyes", "Billie Eilish"),
    ("Lovely", "Billie Eilish"),
    ("What Was I Made For?", "Billie Eilish"),
    ("when the party's over", "Billie Eilish"),
    ("bury a friend", "Billie Eilish"),
    ("Therefore I Am", "Billie Eilish"),
    ("No Time To Die", "Billie Eilish"),

    # Ariana Grande
    ("7 rings", "Ariana Grande"),
    ("thank u, next", "Ariana Grande"),
    ("positions", "Ariana Grande"),
    ("Side To Side", "Ariana Grande"),
    ("God is a woman", "Ariana Grande"),
    ("Into You", "Ariana Grande"),
    ("no tears left to cry", "Ariana Grande"),
    ("One Last Time", "Ariana Grande"),
    ("Problem", "Ariana Grande"),
    ("Break Free", "Ariana Grande"),

    # Post Malone
    ("Circles", "Post Malone"),
    ("Sunflower", "Post Malone"),
    ("rockstar", "Post Malone"),
    ("Congratulations", "Post Malone"),
    ("Psycho", "Post Malone"),
    ("I Fall Apart", "Post Malone"),
    ("White Iverson", "Post Malone"),
    ("Better Now", "Post Malone"),
    ("Wow.", "Post Malone"),
    ("Goodbyes", "Post Malone"),

    # Harry Styles
    ("As It Was", "Harry Styles"),
    ("Watermelon Sugar", "Harry Styles"),
    ("Sign of the Times", "Harry Styles"),
    ("Adore You", "Harry Styles"),
    ("Golden", "Harry Styles"),
    ("Late Night Talking", "Harry Styles"),
    ("Falling", "Harry Styles"),
    ("Music For a Sushi Restaurant", "Harry Styles"),
    ("Kiwi", "Harry Styles"),
    ("Treat People With Kindness", "Harry Styles"),

    # Dua Lipa
    ("Levitating", "Dua Lipa"),
    ("Don't Start Now", "Dua Lipa"),
    ("New Rules", "Dua Lipa"),
    ("Physical", "Dua Lipa"),
    ("Break My Heart", "Dua Lipa"),
    ("One Kiss", "Dua Lipa"),
    ("IDGAF", "Dua Lipa"),
    ("Houdini", "Dua Lipa"),
    ("Be The One", "Dua Lipa"),
    ("Cold Heart", "Dua Lipa"),

    # Olivia Rodrigo
    ("drivers license", "Olivia Rodrigo"),
    ("good 4 u", "Olivia Rodrigo"),
    ("vampire", "Olivia Rodrigo"),
    ("traitor", "Olivia Rodrigo"),
    ("deja vu", "Olivia Rodrigo"),
    ("get him back!", "Olivia Rodrigo"),
    ("brutal", "Olivia Rodrigo"),
    ("happier", "Olivia Rodrigo"),
    ("jealousy, jealousy", "Olivia Rodrigo"),
    ("favorite crime", "Olivia Rodrigo"),

    # Travis Scott
    ("SICKO MODE", "Travis Scott"),
    ("goosebumps", "Travis Scott"),
    ("HIGHEST IN THE ROOM", "Travis Scott"),
    ("Antidote", "Travis Scott"),
    ("Butterfly Effect", "Travis Scott"),
    ("FE!N", "Travis Scott"),
    ("MY EYES", "Travis Scott"),
    ("STARGAZING", "Travis Scott"),
    ("90210", "Travis Scott"),
    ("The Ends", "Travis Scott"),

    # Justin Bieber
    ("Peaches", "Justin Bieber"),
    ("Stay", "Justin Bieber"),
    ("Love Yourself", "Justin Bieber"),
    ("Sorry", "Justin Bieber"),
    ("What Do You Mean?", "Justin Bieber"),
    ("Ghost", "Justin Bieber"),
    ("Intentions", "Justin Bieber"),
    ("Yummy", "Justin Bieber"),
    ("Baby", "Justin Bieber"),
    ("Boyfriend", "Justin Bieber"),

    # BTS
    ("Dynamite", "BTS"),
    ("Butter", "BTS"),
    ("Permission to Dance", "BTS"),
    ("Boy With Luv", "BTS"),
    ("Life Goes On", "BTS"),
    ("DNA", "BTS"),
    ("Fake Love", "BTS"),
    ("MIC Drop", "BTS"),
    ("IDOL", "BTS"),
    ("Spring Day", "BTS"),

    # Doja Cat
    ("Say So", "Doja Cat"),
    ("Woman", "Doja Cat"),
    ("Kiss Me More", "Doja Cat"),
    ("Need To Know", "Doja Cat"),
    ("Paint The Town Red", "Doja Cat"),
    ("Streets", "Doja Cat"),
    ("Boss Bitch", "Doja Cat"),
    ("Like That", "Doja Cat"),
    ("Juicy", "Doja Cat"),
    ("Get Into It (Yuh)", "Doja Cat"),

    # Coldplay
    ("Yellow", "Coldplay"),
    ("Viva La Vida", "Coldplay"),
    ("The Scientist", "Coldplay"),
    ("Fix You", "Coldplay"),
    ("A Sky Full of Stars", "Coldplay"),
    ("Paradise", "Coldplay"),
    ("Hymn for the Weekend", "Coldplay"),
    ("Adventure of a Lifetime", "Coldplay"),
    ("Clocks", "Coldplay"),
    ("Something Just Like This", "Coldplay"),

    # Imagine Dragons
    ("Believer", "Imagine Dragons"),
    ("Thunder", "Imagine Dragons"),
    ("Radioactive", "Imagine Dragons"),
    ("Demons", "Imagine Dragons"),
    ("Whatever It Takes", "Imagine Dragons"),
    ("Enemy", "Imagine Dragons"),
    ("Bones", "Imagine Dragons"),
    ("It's Time", "Imagine Dragons"),
    ("On Top of the World", "Imagine Dragons"),
    ("Natural", "Imagine Dragons"),

    # Kendrick Lamar
    ("HUMBLE.", "Kendrick Lamar"),
    ("DNA.", "Kendrick Lamar"),
    ("LOVE.", "Kendrick Lamar"),
    ("Swimming Pools (Drank)", "Kendrick Lamar"),
    ("m.A.A.d city", "Kendrick Lamar"),
    ("Alright", "Kendrick Lamar"),
    ("Money Trees", "Kendrick Lamar"),
    ("King Kunta", "Kendrick Lamar"),
    ("Poetic Justice", "Kendrick Lamar"),
    ("Bitch Don't Kill My Vibe", "Kendrick Lamar"),

    # SZA
    ("Kill Bill", "SZA"),
    ("Snooze", "SZA"),
    ("All The Stars", "SZA"),
    ("The Weekend", "SZA"),
    ("Good Days", "SZA"),
    ("I Hate U", "SZA"),
    ("Love Galore", "SZA"),
    ("Broken Clocks", "SZA"),
    ("Drew Barrymore", "SZA"),
    ("Garden (Say It Like Dat)", "SZA"),

    # Metro Boomin
    ("Creepin'", "Metro Boomin"),
    ("Superhero", "Metro Boomin"),
    ("Raindrops", "Metro Boomin"),
    ("Feel The Fiyaaaah", "Metro Boomin"),
    ("Too Many Nights", "Metro Boomin"),
    ("Borrowed Love", "Metro Boomin"),
    ("Walk Em Down", "Metro Boomin"),
    ("Trance", "Metro Boomin"),
    ("Ocean Drive", "Metro Boomin"),
    ("BBL Drizzy", "Metro Boomin"),

    # 21 Savage
    ("a lot", "21 Savage"),
    ("Bank Account", "21 Savage"),
    ("Rockstar", "21 Savage"),
    ("X", "21 Savage"),
    ("Redrum", "21 Savage"),
    ("Runnin", "21 Savage"),
    ("Mr. Right Now", "21 Savage"),
    ("Immortal", "21 Savage"),
    ("No Heart", "21 Savage"),
    ("Snitches & Rats", "21 Savage"),

    # Peso Pluma
    ("Ella Baila Sola", "Peso Pluma"),
    ("La Bebe", "Peso Pluma"),
    ("PRC", "Peso Pluma"),
    ("El Azul", "Peso Pluma"),
    ("Siempre Pendientes", "Peso Pluma"),
    ("AMG", "Peso Pluma"),
    ("Por Las Noches", "Peso Pluma"),
    ("Lady Gaga", "Peso Pluma"),
    ("Rubicon", "Peso Pluma"),
    ("El Belicón", "Peso Pluma"),

    # Feid
    ("BRICKELL", "Feid"),
    ("Feliz Cumpleaños Ferxxo", "Feid"),
    ("Si Tú Supieras", "Feid"),
    ("NORMAL", "Feid"),
    ("Fumeteo", "Feid"),
    ("X20X", "Feid"),
    ("Porfa", "Feid"),
    ("Bubalu", "Feid"),
    ("LUNA", "Feid"),
    ("Classy 101", "Feid"),

    # Karol G
    ("TQG", "Karol G"),
    ("Provenza", "Karol G"),
    ("MAMIII", "Karol G"),
    ("Tusa", "Karol G"),
    ("Bichota", "Karol G"),
    ("Cairo", "Karol G"),
    ("Ay, Dios Mío!", "Karol G"),
    ("Mi Cama", "Karol G"),
    ("China", "Karol G"),
    ("200 Copas", "Karol G"),

    # Rauw Alejandro
    ("Todo de Ti", "Rauw Alejandro"),
    ("Tattoo", "Rauw Alejandro"),
    ("Punto 40", "Rauw Alejandro"),
    ("Fantasías", "Rauw Alejandro"),
    ("Lokera", "Rauw Alejandro"),
    ("Dile a Él", "Rauw Alejandro"),
    ("Santa", "Rauw Alejandro"),
    ("Ale Ale", "Rauw Alejandro"),
    ("Cosa Guapa", "Rauw Alejandro"),
    ("Sexy We Japi", "Rauw Alejandro"),

    # J Balvin
    ("Mi Gente", "J Balvin"),
    ("Ay Vamos", "J Balvin"),
    ("Ginza", "J Balvin"),
    ("Safari", "J Balvin"),
    ("Poblado", "J Balvin"),
    ("Morado", "J Balvin"),
    ("Rojo", "J Balvin"),
    ("Azul", "J Balvin"),
    ("7 de Mayo", "J Balvin"),
    ("Qué Más Pues?", "J Balvin"),

    # Shakira
    ("Hips Don't Lie", "Shakira"),
    ("Waka Waka", "Shakira"),
    ("Whenever, Wherever", "Shakira"),
    ("Chantaje", "Shakira"),
    ("La Tortura", "Shakira"),
    ("Can't Remember to Forget You", "Shakira"),
    ("Loca", "Shakira"),
    ("She Wolf", "Shakira"),
    ("Rabiosa", "Shakira"),
    ("BZRP Music Sessions #53", "Shakira"),

    # Bizarrap
    ("Quevedo: Bzrp Music Sessions, Vol. 52", "Bizarrap"),
    ("BZRP Music Sessions #53", "Bizarrap"),
    ("Nicky Jam: Bzrp Music Sessions, Vol. 41", "Bizarrap"),
    ("Residente: Bzrp Music Sessions, Vol. 49", "Bizarrap"),
    ("Nathy Peluso: Bzrp Music Sessions, Vol. 36", "Bizarrap"),
    ("L-Gante: Bzrp Music Sessions, Vol. 38", "Bizarrap"),
    ("Trueno: Bzrp Music Sessions, Vol. 6", "Bizarrap"),
    ("Paulo Londra: Bzrp Music Sessions, Vol. 23", "Bizarrap"),
    ("Duki: Bzrp Music Sessions, Vol. 50", "Bizarrap"),
    ("Villano Antillano: Bzrp Music Sessions, Vol. 51", "Bizarrap"),

    # Miley Cyrus
    ("Flowers", "Miley Cyrus"),
    ("Wrecking Ball", "Miley Cyrus"),
    ("Party in the U.S.A.", "Miley Cyrus"),
    ("Midnight Sky", "Miley Cyrus"),
    ("Malibu", "Miley Cyrus"),
    ("We Can't Stop", "Miley Cyrus"),
    ("The Climb", "Miley Cyrus"),
    ("Slide Away", "Miley Cyrus"),
    ("Prisoner", "Miley Cyrus"),
    ("River", "Miley Cyrus"),

    # Sam Smith
    ("Unholy", "Sam Smith"),
    ("Stay With Me", "Sam Smith"),
    ("Too Good At Goodbyes", "Sam Smith"),
    ("I'm Not the Only One", "Sam Smith"),
    ("Latch", "Sam Smith"),
    ("Dancing With A Stranger", "Sam Smith"),
    ("How Do You Sleep?", "Sam Smith"),
    ("Promises", "Sam Smith"),
    ("Like I Can", "Sam Smith"),
    ("La La La", "Sam Smith"),

    # David Guetta
    ("Titanium", "David Guetta"),
    ("When Love Takes Over", "David Guetta"),
    ("Without You", "David Guetta"),
    ("Memories", "David Guetta"),
    ("Hey Mama", "David Guetta"),
    ("I'm Good (Blue)", "David Guetta"),
    ("2U", "David Guetta"),
    ("Play Hard", "David Guetta"),
    ("Where Them Girls At", "David Guetta"),
    ("Sexy Bitch", "David Guetta"),

    # Calvin Harris
    ("Summer", "Calvin Harris"),
    ("This Is What You Came For", "Calvin Harris"),
    ("Feel So Close", "Calvin Harris"),
    ("We Found Love", "Calvin Harris"),
    ("One Kiss", "Calvin Harris"),
    ("Sweet Nothing", "Calvin Harris"),
    ("How Deep Is Your Love", "Calvin Harris"),
    ("Feels", "Calvin Harris"),
    ("Outside", "Calvin Harris"),
    ("Giant", "Calvin Harris"),

    # Marshmello
    ("Happier", "Marshmello"),
    ("Alone", "Marshmello"),
    ("Silence", "Marshmello"),
    ("Friends", "Marshmello"),
    ("Wolves", "Marshmello"),
    ("Here With Me", "Marshmello"),
    ("One Thing Right", "Marshmello"),
    ("Leave Before You Love Me", "Marshmello"),
    ("Ritual", "Marshmello"),
    ("Be Kind", "Marshmello"),

    # Beyoncé
    ("Halo", "Beyoncé"),
    ("Crazy in Love", "Beyoncé"),
    ("Single Ladies", "Beyoncé"),
    ("Irreplaceable", "Beyoncé"),
    ("Love On Top", "Beyoncé"),
    ("Drunk in Love", "Beyoncé"),
    ("Formation", "Beyoncé"),
    ("Run the World", "Beyoncé"),
    ("If I Were a Boy", "Beyoncé"),
    ("Beautiful Liar", "Beyoncé"),

    # Rihanna
    ("Umbrella", "Rihanna"),
    ("Diamonds", "Rihanna"),
    ("Work", "Rihanna"),
    ("We Found Love", "Rihanna"),
    ("Stay", "Rihanna"),
    ("Love the Way You Lie", "Rihanna"),
    ("Only Girl", "Rihanna"),
    ("Disturbia", "Rihanna"),
    ("Rude Boy", "Rihanna"),
    ("S&M", "Rihanna"),
]

# Extender la lista a 1000 canciones duplicando y agregando variantes
base_songs = songs.copy()
songs = []

# Crear variantes de las canciones (Remix, Live, Acoustic, etc.)
variants = ["", " - Remix", " - Live", " - Acoustic", " - Radio Edit", " - Extended"]

for i in range(1000):
    original_song = base_songs[i % len(base_songs)]
    variant = variants[i // len(base_songs)] if i >= len(base_songs) else ""
    songs.append((original_song[0] + variant, original_song[1]))

# Generar 20000 usuarios
users = [f"user_{i:05d}" for i in range(1, 20001)]

# Generar IPs aleatorias
def generate_ip():
    return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"

# Generar timestamp aleatorio (último año)
def generate_timestamp():
    start_date = datetime.datetime(2024, 1, 1)
    end_date = datetime.datetime(2025, 11, 4)
    time_between = end_date - start_date
    random_days = random.randint(0, time_between.days)
    random_seconds = random.randint(0, 86400)
    random_date = start_date + datetime.timedelta(days=random_days, seconds=random_seconds)
    return random_date.strftime("%Y-%m-%d %H:%M:%S")

# Generar minutos reproducidos (entre 0.5 y 8 minutos)
def generate_minutes():
    return round(random.uniform(0.5, 8.0), 2)

# Función para generar una línea de log
def generate_log_line():
    user = random.choice(users)
    timestamp = generate_timestamp()
    song, artist = random.choice(songs)
    minutes = generate_minutes()
    ip = generate_ip()

    # Formato: usuario|timestamp|cancion|artista|minutos|ip
    return f"{user}|{timestamp}|{song}|{artist}|{minutes}|{ip}\n"

# Generar el archivo
print(f"Generando archivo de logs de Spotify de {TARGET_SIZE_GB}GB...")
print(f"Destino: {OUTPUT_FILE}")
print(f"Total de canciones: {len(songs)}")
print(f"Total de usuarios: {len(users)}")

current_size = 0
lines_written = 0

# Escribir header
header = "usuario|timestamp|cancion|artista|minutos_reproducidos|ip\n"

with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(header)
    current_size += len(header.encode('utf-8'))

    # Escribir líneas hasta alcanzar el tamaño deseado
    while current_size < TARGET_SIZE_BYTES:
        log_line = generate_log_line()
        f.write(log_line)
        current_size += len(log_line.encode('utf-8'))
        lines_written += 1

        # Mostrar progreso cada 100,000 líneas
        if lines_written % 100000 == 0:
            progress = (current_size / TARGET_SIZE_BYTES) * 100
            size_mb = current_size / (1024 * 1024)
            print(f"Progreso: {progress:.1f}% - {size_mb:.0f}MB - {lines_written:,} líneas")

final_size_mb = current_size / (1024 * 1024)
final_size_gb = current_size / (1024 * 1024 * 1024)
print(f"\n¡Archivo generado exitosamente!")
print(f"Tamaño final: {final_size_gb:.2f}GB ({final_size_mb:.0f}MB)")
print(f"Total de líneas: {lines_written:,}")
print(f"Archivo: {OUTPUT_FILE}")
