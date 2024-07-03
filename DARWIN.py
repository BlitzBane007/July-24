import pygame

# Initialize Pygame
pygame.init()

# Set up some constants
WIDTH, HEIGHT = 720, 720
PLAYER_SIZE = 10
PLAYER1_POS = [50, 50]
PLAYER2_POS = [100, 100]

# Set up the display
screen = pygame.display.set_mode((WIDTH, HEIGHT))

# Function to draw players
def draw_players():
    pygame.draw.rect(screen, (255, 0, 0), (PLAYER1_POS[0], PLAYER1_POS[1], PLAYER_SIZE, PLAYER_SIZE))
    pygame.draw.rect(screen, (0, 255, 0), (PLAYER2_POS[0], PLAYER2_POS[1], PLAYER_SIZE, PLAYER_SIZE))

# Function to display text
def display_text(text, pos):
    font = pygame.font.Font(None, 36)
    text = font.render(text, 1, (10, 10, 10))
    screen.blit(text, pos)

# Game loop
running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False

    screen.fill((255, 255, 255))
    draw_players()
    display_text("Hello", (10, 10))
    pygame.display.flip()

pygame.quit()
