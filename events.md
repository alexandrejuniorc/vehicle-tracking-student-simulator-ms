# Eventos

## Vamos receber

RouteCreated

- id
- distance
- directions
- - lat
- - lng

### Efeito colateral (Calcular o frete e retornar o evento)

FreightCalculated

- route_id
- amount

---

# Vamos receber

DeliveryStarted

- route_id

### Efeito colateral

DriverMoved

- route_id
- lat
- lng
